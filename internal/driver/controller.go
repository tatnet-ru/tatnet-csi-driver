package driver

import (
	"context"
	"sync"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"

	"github.com/tatnet-ru/tatnet-csi-driver/internal/tatnet"
)

// ContextSerial — ключ, под которым серийник тома едет в volume_context от
// контроллера к node-плагину. Это единственная связь между идентификатором
// тома в платформе и устройством на ноде, и передаётся она явно: выводить её
// на ноде повторно значило бы завести второе место, где закодировано одно
// правило.
const ContextSerial = "tatnet.ru/device-serial"

// gibibyte — тома платформы меряются в ГиБ, CSI — в байтах.
const gibibyte = 1 << 30

type controllerServer struct {
	csi.UnimplementedControllerServer
	cfg          Config
	api          *tatnet.Client
	operations   sync.Map
	pollInterval time.Duration
}

func (s *controllerServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
	out := make([]*csi.ControllerServiceCapability, 0, len(caps))
	for _, c := range caps {
		out = append(out, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: c}},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: out}, nil
}

// sizeGB переводит запрошенный CSI объём в ГиБ платформы, округляя ВВЕРХ:
// отдать меньше запрошенного нельзя, а лишний гигабайт безвреден.
func sizeGB(cr *csi.CapacityRange) int {
	if cr == nil || cr.GetRequiredBytes() <= 0 {
		return 1
	}
	gb := int((cr.GetRequiredBytes() + gibibyte - 1) / gibibyte)
	if gb < 1 {
		gb = 1
	}
	return gb
}

// CreateVolume идемпотентен по ИМЕНИ, а не по попытке.
//
// CSI ретраится по построению: тот же вызов приходит повторно после любого
// таймаута. Без поиска по имени повтор завёл бы второй LV, о котором никто не
// знает и место с которого не вернётся никогда. Уникальность имени в проекте
// гарантирует платформа, но полагаться только на её отказ нельзя — тогда
// повтор возвращал бы ошибку вместо уже созданного тома.
func (s *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name обязателен")
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, err
	}
	want := sizeGB(req.GetCapacityRange())

	vol, err := s.api.FindByName(ctx, name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "поиск тома %s: %v", name, err)
	}
	if vol == nil {
		vol, err = s.api.Create(ctx, name, s.cfg.RegionID, want)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "создание тома %s: %v", name, err)
		}
		klog.InfoS("том создан", "name", name, "id", vol.ID, "gb", want)
	} else if vol.SizeGB < want {
		// Тот же PVC пересоздают с бо́льшим размером: отдать меньший том
		// молча — значит соврать о вместимости.
		return nil, status.Errorf(codes.AlreadyExists,
			"том %s существует на %d ГиБ, запрошено %d", name, vol.SizeGB, want)
	}

	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      vol.ID,
		CapacityBytes: int64(vol.SizeGB) * gibibyte,
		VolumeContext: map[string]string{ContextSerial: vol.DeviceSerial},
		// Том привязан к региону: LV лежит на SAN своего кластера, и нода
		// другого региона его не откроет. Планировщик обязан это знать.
		AccessibleTopology: []*csi.Topology{
			{Segments: map[string]string{TopologyKeyRegion: vol.Region}},
		},
	}}, nil
}

func (s *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id обязателен")
	}
	if err := s.api.Delete(ctx, req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.Internal, "удаление тома %s: %v", req.GetVolumeId(), err)
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume прикрепляет том к машине ноды.
//
// Возвращает серийник в publish_context, хотя он же есть в volume_context: у
// PV, созданного не через CreateVolume (статический provisioning), контекста
// тома нет, а прикрепление всё равно проходит здесь.
func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	id, nodeID := req.GetVolumeId(), req.GetNodeId()
	if id == "" || nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id и node_id обязательны")
	}
	if err := validateCapabilities([]*csi.VolumeCapability{req.GetVolumeCapability()}); err != nil {
		return nil, err
	}
	if !s.startOperation(id) {
		return nil, status.Errorf(codes.Aborted, "операция с томом %s уже выполняется", id)
	}
	defer s.operations.Delete(id)

	vmID, err := s.resolveNode(ctx, nodeID)
	if err != nil {
		return nil, err
	}

	vol, err := s.api.Get(ctx, id)
	if err != nil {
		if tatnet.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "том %s не найден", id)
		}
		return nil, status.Errorf(codes.Internal, "чтение тома %s: %v", id, err)
	}
	if !vol.Ready() {
		// Том ещё создаётся или сломан. Ретраить осмысленно только первое,
		// но различать здесь нечем — external-attacher повторит в обоих
		// случаях, а причина видна в error_detail.
		return nil, status.Errorf(codes.Unavailable,
			"том %s не готов (%s): %s", id, vol.Status, vol.ErrorDetail)
	}
	if a := vol.Attachment; a != nil {
		if a.VMID != vmID {
			return nil, status.Errorf(codes.FailedPrecondition,
				"том %s уже прикреплён к машине %s", id, a.VMID)
		}
		if a.DesiredState == "absent" || a.Status == "detaching" {
			return nil, status.Errorf(codes.Aborted, "том %s ещё открепляется", id)
		}
		if a.Status == "attached" {
			return publishResponse(vol), nil
		}
	}

	// API accepts an asynchronous intent. Only submit when no operation is
	// pending, or retry a failed attach to the SAME VM. Never detach to retry.
	if vol.Attachment == nil || vol.Attachment.Status == "error" {
		vol, err = s.api.Attach(ctx, id, vmID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "прикрепление тома %s к %s: %v", id, vmID, err)
		}
	}
	vol, err = s.waitVolume(ctx, id, func(v *tatnet.Volume) (bool, error) {
		if v == nil {
			return false, status.Errorf(codes.NotFound, "том %s исчез при прикреплении", id)
		}
		a := v.Attachment
		if a == nil {
			return false, nil
		}
		if a.VMID != vmID {
			return false, status.Errorf(codes.FailedPrecondition, "том %s занят машиной %s", id, a.VMID)
		}
		if a.DesiredState == "absent" || a.Status == "detaching" {
			return false, status.Errorf(codes.Aborted, "том %s открепляется", id)
		}
		if a.Status == "error" {
			return false, status.Errorf(codes.Internal, "прикрепление тома %s: %s", id, a.ErrorDetail)
		}
		return a.Status == "attached", nil
	})
	if err != nil {
		return nil, err
	}
	klog.InfoS("том прикреплён", "volume", id, "node", nodeID, "vm", vmID)
	return publishResponse(vol), nil
}

func publishResponse(vol *tatnet.Volume) *csi.ControllerPublishVolumeResponse {
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: map[string]string{ContextSerial: vol.DeviceSerial},
	}
}

// ControllerUnpublishVolume открепляет том.
//
// К этому моменту NodeUnstageVolume уже размонтировал ФС — Kubernetes
// гарантирует такой порядок. Именно поэтому клиент вправе передать платформе
// force: гипервизор гостя не спрашивает, и гарантию даёт управляющая плоскость.
func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id обязателен")
	}
	if !s.startOperation(id) {
		return nil, status.Errorf(codes.Aborted, "операция с томом %s уже выполняется", id)
	}
	defer s.operations.Delete(id)
	vol, err := s.api.Get(ctx, id)
	if tatnet.IsNotFound(err) {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "чтение тома %s: %v", id, err)
	}
	if vol.Attachment == nil {
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	}
	if req.GetNodeId() != "" {
		vmID, err := s.resolveNode(ctx, req.GetNodeId())
		if err != nil {
			return nil, err
		}
		// A stale unpublish for another node must never unplug the new owner.
		if vol.Attachment.VMID != vmID {
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
	}
	if err := s.api.Detach(ctx, id); err != nil {
		return nil, status.Errorf(codes.Internal, "открепление тома %s: %v", id, err)
	}
	if _, err := s.waitVolume(ctx, id, func(v *tatnet.Volume) (bool, error) {
		if v == nil || v.Attachment == nil {
			return true, nil
		}
		if v.Attachment.VMID != vol.Attachment.VMID {
			return false, status.Errorf(codes.Aborted, "прикрепление тома %s изменилось", id)
		}
		if v.Attachment.Status == "error" {
			return false, status.Errorf(codes.Internal, "открепление тома %s: %s", id, v.Attachment.ErrorDetail)
		}
		return false, nil
	}); err != nil {
		return nil, err
	}
	klog.InfoS("том откреплён", "volume", id, "node", req.GetNodeId())
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (s *controllerServer) startOperation(id string) bool {
	_, loaded := s.operations.LoadOrStore(id, struct{}{})
	return !loaded
}

// waitVolume observes provider state, rather than treating an accepted
// asynchronous request as completion. Cancellation does not undo the intent:
// the next CSI call observes/resumes it, as in DO and AWS CSI drivers.
func (s *controllerServer) waitVolume(ctx context.Context, id string, ready func(*tatnet.Volume) (bool, error)) (*tatnet.Volume, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	interval := s.pollInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		v, err := s.api.Get(ctx, id)
		if tatnet.IsNotFound(err) {
			v, err = nil, nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, status.Errorf(codes.Unavailable, "состояние тома %s: %v", id, err)
		}
		done, err := ready(v)
		if err != nil {
			return nil, err
		}
		if done {
			return v, nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
}

// ControllerExpandVolume растит сам том. Файловую систему растит нода — отсюда
// NodeExpansionRequired.
func (s *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	id := req.GetVolumeId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id обязателен")
	}
	want := sizeGB(req.GetCapacityRange())
	vol, err := s.api.Resize(ctx, id, want)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "расширение тома %s: %v", id, err)
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes: int64(vol.SizeGB) * gibibyte,
		// true даже если том сейчас ни к чему не прикреплён: kubelet выполнит
		// расширение ФС при следующем монтировании, а сказать «не требуется»
		// значит навсегда оставить гостя со старой ёмкостью.
		NodeExpansionRequired: true,
	}, nil
}

func (s *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id обязателен")
	}
	if _, err := s.api.Get(ctx, req.GetVolumeId()); err != nil {
		if tatnet.IsNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "том %s не найден", req.GetVolumeId())
		}
		return nil, status.Errorf(codes.Internal, "чтение тома: %v", err)
	}
	if err := validateCapabilities(req.GetVolumeCapabilities()); err != nil {
		// Неподдерживаемый режим — это не ошибка вызова: ответ без
		// confirmed означает «такие возможности я не подтверждаю».
		return &csi.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// validateCapabilities пропускает только то, что блочный том действительно
// умеет: одну ноду на запись.
//
// RWX здесь физически невозможен — это одно устройство с одной файловой
// системой, и two writers это порча ФС, а не «неподдерживаемая фича». Сказать
// «поддерживаю» и надеяться, что клиент не воспользуется, было бы худшим из
// вариантов.
func validateCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume_capabilities обязательны")
	}
	for _, c := range caps {
		switch c.GetAccessMode().GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
		default:
			return status.Errorf(codes.InvalidArgument,
				"режим доступа %s не поддерживается: блочный том даёт одну ноду на запись",
				c.GetAccessMode().GetMode())
		}
	}
	return nil
}

// resolveNode переводит идентификатор ноды CSI (её InternalIP) в машину TatNet.
//
// Адрес, а не имя: общего имени у ноды Kubernetes и машины нет — Kubernetes
// зовёт ноду `talos-b25-9uq`, платформа знает свой hostname. И не файл в
// машинном конфиге: Talos не принимает `machine.files` в immediate-режиме и
// роняет этим ЛЮБУЮ конвергенцию конфига живой ноды, то есть перестают
// доставляться и все остальные аддоны.
//
// Спрашиваем каждый раз, а не держим карту: ноды добавляются и уезжают, и
// снимок, розданный при старте, отказал бы первому же добавленному узлу.
// Прикрепления редки, цена запроса ничтожна.
func (s *controllerServer) resolveNode(ctx context.Context, nodeID string) (string, error) {
	nodes, err := s.api.Nodes(ctx)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "список нод кластера: %v", err)
	}
	for _, n := range nodes {
		if n.Address != "" && n.Address == nodeID && n.VMID != "" {
			return n.VMID, nil
		}
	}
	// НЕ подставляем nodeID как vm_id: платформа приняла бы адрес за
	// идентификатор машины и ответила «машина не найдена» — отказом, из
	// которого не видно, что дело в связке.
	return "", status.Errorf(codes.NotFound,
		"нода %s не найдена среди нод кластера (%d шт.) — привязка к машине неизвестна",
		nodeID, len(nodes))
}
