package driver

import (
	"context"
	"os"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
	utilexec "k8s.io/utils/exec"
)

// TopologyKeyRegion — по нему планировщик понимает, что том доступен не
// отовсюду: LV лежит на SAN своего кластера, и нода другого региона его не
// откроет.
const TopologyKeyRegion = "topology." + DriverName + "/region"

type nodeServer struct {
	csi.UnimplementedNodeServer
	cfg     Config
	mounter *mount.SafeFormatAndMount
}

func newNodeServer(cfg Config) *nodeServer {
	return &nodeServer{
		cfg: cfg,
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New(""),
			Exec:      utilexec.New(),
		},
	}
}

func (s *nodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: s.cfg.NodeID,
		// Слотов у машины vdb…vdz; корневой диск занимает vda.
		MaxVolumesPerNode: maxVolumesPerNode,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{TopologyKeyRegion: s.cfg.RegionID},
		},
	}, nil
}

// maxVolumesPerNode — сколько томов помещается на машину. Не абстрактный
// лимит: у домена libvirt слоты vdb…vdz, а vda занят корневым диском. Соврать
// в большую сторону значит дать планировщику назначить под, который потом
// нельзя будет запустить.
const maxVolumesPerNode = 25

func (s *nodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
	}
	out := make([]*csi.NodeServiceCapability, 0, len(caps))
	for _, c := range caps {
		out = append(out, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{Rpc: &csi.NodeServiceCapability_RPC{Type: c}},
		})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: out}, nil
}

// NodeStageVolume находит устройство, при необходимости создаёт на нём ФС и
// монтирует в staging-каталог — один раз на ноду, сколько бы подов том ни
// использовали.
func (s *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	target := req.GetStagingTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id и staging_target_path обязательны")
	}
	cap := req.GetVolumeCapability()
	if cap == nil {
		return nil, status.Error(codes.InvalidArgument, "volume_capability обязателен")
	}
	// Raw-block отдаётся поду как есть: ни форматировать, ни монтировать здесь
	// нечего, всё делает NodePublishVolume.
	if cap.GetBlock() != nil {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	serial := req.GetVolumeContext()[ContextSerial]
	if serial == "" {
		// Серийник кладёт контроллер в volume_context при создании тома. Его
		// отсутствие означает, что PV сделан не нами или контекст потерян —
		// угадывать устройство нельзя.
		return nil, status.Error(codes.FailedPrecondition,
			"в контексте тома нет серийника — устройство не опознать")
	}
	device, err := FindDevice(serial)
	if err != nil {
		// NotFound, а не Internal: external-attacher ждёт, пока устройство
		// доедет, и ретраит. Горячее подключение занимает секунды.
		return nil, status.Errorf(codes.NotFound, "устройство тома: %v", err)
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "создать %s: %v", target, err)
	}
	notMounted, err := s.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "проверить %s: %v", target, err)
	}
	if !notMounted {
		return &csi.NodeStageVolumeResponse{}, nil // уже смонтировано — идемпотентность
	}

	fsType := cap.GetMount().GetFsType()
	if fsType == "" {
		fsType = "ext4"
	}
	opts := cap.GetMount().GetMountFlags()
	klog.InfoS("монтирую том", "volume", req.GetVolumeId(), "device", device, "fs", fsType)
	// FormatAndMount создаёт ФС ТОЛЬКО на пустом устройстве: том с чужими
	// данными он монтирует как есть, а не переформатирует.
	if err := s.mounter.FormatAndMount(device, target, fsType, opts); err != nil {
		return nil, status.Errorf(codes.Internal, "смонтировать %s на %s: %v", device, target, err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume размонтирует staging-каталог.
//
// Это и есть тот момент, на котором держится безопасность всей схемы: Kubernetes
// зовёт его ДО ControllerUnpublishVolume, поэтому к моменту отключения диска в
// гипервизоре ФС уже не смонтирована. Сам гипервизор такой гарантии не даёт —
// он вырывает живой virtio-диск молча.
func (s *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	target := req.GetStagingTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id и staging_target_path обязательны")
	}
	if err := mount.CleanupMountPoint(target, s.mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "размонтировать %s: %v", target, err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume пробрасывает staging-каталог в каталог пода bind-монтом.
func (s *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	source, target := req.GetStagingTargetPath(), req.GetTargetPath()
	if req.GetVolumeId() == "" || source == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id, staging_target_path и target_path обязательны")
	}
	if err := os.MkdirAll(target, 0o750); err != nil {
		return nil, status.Errorf(codes.Internal, "создать %s: %v", target, err)
	}
	notMounted, err := s.mounter.IsLikelyNotMountPoint(target)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Errorf(codes.Internal, "проверить %s: %v", target, err)
	}
	if !notMounted {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	opts := []string{"bind"}
	if req.GetReadonly() {
		opts = append(opts, "ro")
	}
	opts = append(opts, req.GetVolumeCapability().GetMount().GetMountFlags()...)
	if err := s.mounter.Mount(source, target, "", opts); err != nil {
		return nil, status.Errorf(codes.Internal, "bind-монт %s → %s: %v", source, target, err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id и target_path обязательны")
	}
	if err := mount.CleanupMountPoint(target, s.mounter, true); err != nil {
		return nil, status.Errorf(codes.Internal, "размонтировать %s: %v", target, err)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeExpandVolume растит файловую систему после того, как контроллер вырастил
// сам том. Без этого шага место есть, а гость его не видит: расширение
// «прошло», а df тот же.
func (s *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	path := req.GetVolumePath()
	if req.GetVolumeId() == "" || path == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id и volume_path обязательны")
	}
	device, _, err := mount.GetDeviceNameFromMount(s.mounter, path)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "устройство под %s: %v", path, err)
	}
	if device == "" {
		return nil, status.Errorf(codes.NotFound, "под %s ничего не смонтировано", path)
	}
	resizer := mount.NewResizeFs(s.mounter.Exec)
	if _, err := resizer.Resize(device, path); err != nil {
		return nil, status.Errorf(codes.Internal, "расширить ФС на %s: %v", device, err)
	}
	return &csi.NodeExpandVolumeResponse{}, nil
}
func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\n' || s[start] == '\r' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\n' || s[end-1] == '\r' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
