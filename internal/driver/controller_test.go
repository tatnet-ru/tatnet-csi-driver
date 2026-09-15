package driver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tatnet-ru/tatnet-csi-driver/internal/tatnet"
	api "github.com/tatnet-ru/tatnet-go/tatnet"
)

const (
	testK8sCluster = "3e4ac029-0f95-4a72-ad9d-683a9f798a43"
	nodeIP1        = "10.7.1.153"
	nodeIP2        = "10.7.1.92"
	testProject    = "175a32c7-efe2-426d-a492-0e2725c26f94"
	testRegion     = "58fa93f8-34ad-4a27-acae-273baf13b472"
	testVolID      = "a91bd4aa-8c9b-420f-bb44-544284c9321a"
	testSerial     = "a91bd4aa8c9b420fbb44"
)

// fakeAPI — подставная платформа. Считает вызовы, чтобы тесты могли проверять
// не только ответ, но и то, сколько раз мы сходили наружу: идемпотентность
// CreateVolume — это в первую очередь «второй том НЕ создан».
type fakeAPI struct {
	volumes map[string]*api.V1Volume
	creates int
	// lastCreate — кластер из последнего запроса на создание. Без него api
	// вешает ссылку тома на проект, которого скоуп ключа не покрывает, и
	// создание отказывает 403-м.
	lastCreate string
	// nodes — связка «адрес ноды → машина», которую контроллер спрашивает у
	// платформы вместо того, чтобы держать снимок.
	nodes       []api.V1K8sNode
	nodeLookups int
	attach      int
	detach      int
	srv         *httptest.Server
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	f := &fakeAPI{
		volumes: map[string]*api.V1Volume{},
		nodes: []api.V1K8sNode{
			{VmId: ptr("vm-1"), Address: ptr(nodeIP1), Role: "worker", Status: "ready"},
			{VmId: ptr("vm-2"), Address: ptr(nodeIP2), Role: "worker", Status: "ready"},
		},
	}
	mux := http.NewServeMux()
	// Настоящий API отвечает application/json, и сгенерированный клиент без
	// этого заголовка тело НЕ разбирает. Раньше рукописный клиент парсил что
	// угодно, поэтому двойник мог его не ставить — и не ставил.
	jsonHdr := func(w http.ResponseWriter) { w.Header().Set("Content-Type", "application/json") }
	base := "/v1/projects/" + testProject + "/volumes"

	mux.HandleFunc(base, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			var list []api.V1Volume
			for _, v := range f.volumes {
				list = append(list, *v)
			}
			jsonHdr(w)
			json.NewEncoder(w).Encode(map[string]any{"data": list})
		case http.MethodPost:
			var body struct {
				Name       string `json:"name"`
				Region     string `json:"region_id"`
				SizeGB     int    `json:"size_gb"`
				K8sCluster string `json:"k8s_cluster_id"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			f.creates++
			f.lastCreate = body.K8sCluster
			v := &api.V1Volume{
				Id: testVolID, Name: body.Name, RegionId: body.Region,
				SizeGb: body.SizeGB, Status: "creating", DeviceSerial: testSerial,
			}
			f.volumes[v.Id] = v
			jsonHdr(w)
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(v)
		}
	})
	mux.HandleFunc("/v1/projects/"+testProject+"/kubernetes-clusters/"+testK8sCluster+"/nodes",
		func(w http.ResponseWriter, r *http.Request) {
			f.nodeLookups++
			jsonHdr(w)
			json.NewEncoder(w).Encode(f.nodes)
		})

	mux.HandleFunc(base+"/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, base+"/")
		id, action, _ := strings.Cut(rest, "/")
		v, ok := f.volumes[id]
		if !ok {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		switch action {
		case "attach":
			var body struct {
				VMID string `json:"vm_id"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			f.attach++
			v.Attachment = &api.V1VolumeAttachment{VmId: body.VMID, Status: "attaching"}
			jsonHdr(w)
			json.NewEncoder(w).Encode(v)
		case "detach":
			f.detach++
			v.Attachment = nil
			w.WriteHeader(http.StatusOK)
		case "":
			switch r.Method {
			case http.MethodDelete:
				delete(f.volumes, id)
				w.WriteHeader(http.StatusAccepted)
			case http.MethodPatch:
				var body struct {
					SizeGB int `json:"size_gb"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				v.SizeGb = body.SizeGB
				jsonHdr(w)
				json.NewEncoder(w).Encode(v)
			default:
				jsonHdr(w)
				json.NewEncoder(w).Encode(v)
			}
		}
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newController(t *testing.T, f *fakeAPI) *controllerServer {
	t.Helper()
	api, err := tatnet.NewClient(f.srv.URL, testProject, testK8sCluster, "tn_live_test")
	if err != nil {
		t.Fatalf("клиент tatnet: %v", err)
	}
	return &controllerServer{
		cfg: Config{RegionID: testRegion, K8sClusterID: testK8sCluster},
		api: api,
	}
}

func mountCaps() []*csi.VolumeCapability {
	return []*csi.VolumeCapability{{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{FsType: "ext4"}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}}
}

// CSI ретраится по построению: тот же вызов приходит повторно после любого
// таймаута. Без поиска по имени повтор завёл бы ВТОРОЙ LV, о котором никто не
// знает и место с которого не вернётся.
func TestCreateVolume_RetryDoesNotAllocateTwice(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	req := &csi.CreateVolumeRequest{
		Name:               "pvc-abc",
		VolumeCapabilities: mountCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 2 * gibibyte},
	}
	first, err := c.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	second, err := c.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatalf("повтор CreateVolume: %v", err)
	}
	if f.creates != 1 {
		t.Errorf("создано томов: %d, ожидался 1", f.creates)
	}
	if first.Volume.VolumeId != second.Volume.VolumeId {
		t.Errorf("повтор вернул другой том: %s vs %s", first.Volume.VolumeId, second.Volume.VolumeId)
	}
}

// Серийник — единственная связь между томом платформы и устройством на ноде.
// Без него node-плагин не найдёт диск и монтировать будет нечего.
func TestCreateVolume_CarriesSerialAndTopology(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-abc", VolumeCapabilities: mountCaps(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: gibibyte},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if got := resp.Volume.VolumeContext[ContextSerial]; got != testSerial {
		t.Errorf("серийник = %q, ожидался %q", got, testSerial)
	}
	if len(resp.Volume.AccessibleTopology) != 1 {
		t.Fatal("нет топологии — планировщик сочтёт том доступным из любого региона")
	}
	if got := resp.Volume.AccessibleTopology[0].Segments[TopologyKeyRegion]; got != testRegion {
		t.Errorf("регион в топологии = %q, ожидался %q", got, testRegion)
	}
}

// Округление ВВЕРХ: отдать меньше запрошенного нельзя.
func TestCreateVolume_RoundsCapacityUp(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	resp, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-abc", VolumeCapabilities: mountCaps(),
		CapacityRange: &csi.CapacityRange{RequiredBytes: gibibyte + 1},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if resp.Volume.CapacityBytes != 2*gibibyte {
		t.Errorf("ёмкость = %d, ожидалось %d", resp.Volume.CapacityBytes, 2*gibibyte)
	}
}

// RWX на блочном томе физически невозможен: одно устройство, одна ФС, двое
// пишущих — порча. Сказать «поддерживаю» и надеяться было бы худшим вариантом.
func TestCreateVolume_RefusesMultiNodeAccess(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name: "pvc-rwx",
		VolumeCapabilities: []*csi.VolumeCapability{{
			AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RWX принят (ошибка: %v)", err)
	}
	if f.creates != 0 {
		t.Error("том создан несмотря на неподдерживаемый режим доступа")
	}
}

// Повторное прикрепление к ТОЙ ЖЕ машине — идемпотентный успех; к другой —
// отказ: у тома не может быть двух прикреплений, и молча переехать значило бы
// выдернуть диск из-под работающего пода.
func TestControllerPublish_IdempotentSameNodeRefusesOther(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	f.volumes[testVolID] = &api.V1Volume{
		Id: testVolID, Status: "ready", DeviceSerial: testSerial, RegionId: testRegion,
	}

	if _, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: nodeIP1, VolumeCapability: mountCaps()[0],
	}); err != nil {
		t.Fatalf("первое прикрепление: %v", err)
	}
	if _, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: nodeIP1, VolumeCapability: mountCaps()[0],
	}); err != nil {
		t.Fatalf("повтор к той же машине должен быть успехом: %v", err)
	}
	if f.attach != 1 {
		t.Errorf("прикреплений: %d, ожидалось 1", f.attach)
	}

	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: nodeIP2, VolumeCapability: mountCaps()[0],
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("прикрепление ко второй машине прошло (ошибка: %v)", err)
	}
}

// Неготовый том — Unavailable, а не Internal: external-attacher должен
// ретраить, пока LV создаётся.
func TestControllerPublish_NotReadyIsRetryable(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	f.volumes[testVolID] = &api.V1Volume{Id: testVolID, Status: "creating"}

	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: nodeIP1, VolumeCapability: mountCaps()[0],
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("код = %s, ожидался Unavailable: %v", status.Code(err), err)
	}
}

// Удаление и открепление обязаны быть идемпотентными: CSI повторяет их до
// успеха, и «уже нет» — это успех, а не вечный ретрай.
func TestDeleteAndUnpublish_AreIdempotent(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)

	if _, err := c.DeleteVolume(context.Background(),
		&csi.DeleteVolumeRequest{VolumeId: "нет-такого"}); err != nil {
		t.Errorf("удаление несуществующего тома должно быть успехом: %v", err)
	}
	if _, err := c.ControllerUnpublishVolume(context.Background(),
		&csi.ControllerUnpublishVolumeRequest{VolumeId: "нет-такого", NodeId: "vm-1"}); err != nil {
		t.Errorf("открепление несуществующего тома должно быть успехом: %v", err)
	}
}

// Расширение всегда требует шага на ноде: без роста ФС место есть, а гость его
// не видит — расширение «прошло», а df тот же.
func TestControllerExpand_AlwaysRequiresNodeStep(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	f.volumes[testVolID] = &api.V1Volume{Id: testVolID, Status: "ready", SizeGb: 2}

	resp, err := c.ControllerExpandVolume(context.Background(), &csi.ControllerExpandVolumeRequest{
		VolumeId: testVolID, CapacityRange: &csi.CapacityRange{RequiredBytes: 4 * gibibyte},
	})
	if err != nil {
		t.Fatalf("ControllerExpandVolume: %v", err)
	}
	if !resp.NodeExpansionRequired {
		t.Error("NodeExpansionRequired=false оставит гостя со старой ёмкостью навсегда")
	}
	if resp.CapacityBytes != 4*gibibyte {
		t.Errorf("ёмкость = %d, ожидалось %d", resp.CapacityBytes, 4*gibibyte)
	}
}

// Том обязан уехать в api собственностью КЛАСТЕРА. Без `k8s_cluster_id` api
// вешает ссылку создаваемого тома на проект — а ключ драйвера имеет скоуп
// `k8s_cluster:<id>`, и создание отказывает 403-м, то есть НИ ОДИН PVC не
// поднимется. Здесь же держится и снос кластера: оркестратор помечает на
// удаление именно тома с этой парой.
func TestCreateVolume_ClaimsClusterOwnership(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)

	_, err := c.CreateVolume(context.Background(), &csi.CreateVolumeRequest{
		Name:               "pvc-own",
		VolumeCapabilities: mountCaps(),
		CapacityRange:      &csi.CapacityRange{RequiredBytes: gibibyte},
	})
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if f.lastCreate != testK8sCluster {
		t.Fatalf("в запросе кластер %q, ожидался %q", f.lastCreate, testK8sCluster)
	}
}

// Нода, которой нет среди нод кластера, — это отказ с ВНЯТНОЙ причиной, а не
// попытка выдать адрес за идентификатор машины: платформа ответила бы «машина
// не найдена», и из отказа не было бы видно, что дело в связке.
func TestControllerPublish_UnknownNodeIsRefused(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	f.volumes[testVolID] = &api.V1Volume{
		Id: testVolID, Status: "ready", DeviceSerial: testSerial, RegionId: testRegion,
	}

	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: "10.7.1.200", VolumeCapability: mountCaps()[0],
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("код = %v, ожидался NotFound (ошибка: %v)", status.Code(err), err)
	}
	if f.attach != 0 {
		t.Error("прикрепление всё-таки ушло в платформу")
	}
}

// Связка спрашивается КАЖДЫЙ раз, а не кэшируется при старте: ноды добавляются
// и уезжают, и снимок отказал бы первому же добавленному узлу.
func TestControllerPublish_ResolvesNodeEveryTime(t *testing.T) {
	f := newFakeAPI(t)
	c := newController(t, f)
	f.volumes[testVolID] = &api.V1Volume{
		Id: testVolID, Status: "ready", DeviceSerial: testSerial, RegionId: testRegion,
	}

	// Узел, которого при первом обращении ещё нет.
	_, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: "10.7.1.77", VolumeCapability: mountCaps()[0],
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ожидался NotFound, получено %v", err)
	}

	f.nodes = append(f.nodes, api.V1K8sNode{VmId: ptr("vm-3"), Address: ptr("10.7.1.77"), Status: "ready"})
	if _, err := c.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{
		VolumeId: testVolID, NodeId: "10.7.1.77", VolumeCapability: mountCaps()[0],
	}); err != nil {
		t.Fatalf("добавленный узел не обслужен: %v", err)
	}
	if f.nodeLookups < 2 {
		t.Errorf("связку спросили %d раз — значит она кэширована", f.nodeLookups)
	}
}

func ptr[T any](v T) *T { return &v }
