// Package tatnet — клиент публичного /v1 для блочных томов TatNet.
//
// Транспорт и модели — СГЕНЕРИРОВАННЫЙ клиент github.com/tatnet-ru/tatnet-go,
// порождаемый из контракта /v1. Здесь остаётся только то, что генератор дать
// не может: семантика драйвера (идемпотентность удаления и открепления,
// поиск по имени постранично) и узкие типы, которыми пользуется остальной код.
//
// Раньше запросы и структуры были написаны здесь руками, и цена этого
// записана в родственном клиенте CCM: «список в конверте называется data, НЕ
// items; ошибёшься — FindByName молча теряет балансировщики». Теперь
// расхождение с контрактом — ошибка компиляции, а не тихий ноль.
//
// Ключ проектный (`tn_live_`), выпущен для CSI этого кластера со скоупом
// `k8s_cluster:<id>`. CSI-тома несут кластер в предках ресурса, поэтому такой
// ключ совпадает ровно со своими томами — ни с остальными томами проекта, ни с
// томами соседнего кластера. Та же форма, что у CCM.
//
// Ключ живёт ТОЛЬКО в контроллере. На нодах его нет: node-плагин ничего не
// спрашивает у платформы, он работает с устройством, которое уже принесли.
package tatnet

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	api "github.com/tatnet-ru/tatnet-go/tatnet"
)

// Client — тонкий клиент томов, ограниченный одним проектом ключом.
type Client struct {
	projectID    string
	k8sClusterID string
	gen          *api.ClientWithResponses
}

// NewClient. Возвращает ошибку, потому что сгенерированный конструктор умеет
// отказать (негодный адрес); раньше отказывать было нечему — запрос собирался
// вручную на каждом вызове.
func NewClient(base, projectID, k8sClusterID, key string) (*Client, error) {
	gen, err := api.NewClientWithResponses(
		strings.TrimRight(base, "/")+"/v1",
		api.WithAPIKey(key),
		api.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	)
	if err != nil {
		return nil, fmt.Errorf("клиент tatnet: %w", err)
	}
	return &Client{projectID: projectID, k8sClusterID: k8sClusterID, gen: gen}, nil
}

// Attachment — текущее прикрепление тома, или nil.
type Attachment struct {
	VMID      string
	Status    string
	TargetDev string
}

// Volume — то, что нужно драйверу. Поля заполняются из сгенерированных
// моделей: переименование в контракте ломает сборку здесь, а не поведение в
// проде.
type Volume struct {
	ID     string
	Name   string
	Region string
	SizeGB int
	// ActualSizeGB меньше SizeGB ⇒ расширение ещё идёт. Отдельного статуса
	// «resizing» у платформы нет намеренно, чтобы один факт не жил в двух
	// местах.
	ActualSizeGB int
	Status       string
	DesiredState string
	// DeviceSerial — то, по чему гость найдёт устройство как
	// /dev/disk/by-id/virtio-<serial>. Единственная связь между платформенным
	// идентификатором тома и тем, что видно на ноде.
	DeviceSerial string
	ErrorDetail  string
	Attachment   *Attachment
}

// Ready сообщает, что LV существует и том можно прикреплять.
func (v *Volume) Ready() bool { return v.Status == "ready" }

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

func toVolume(v *api.V1Volume) *Volume {
	if v == nil {
		return nil
	}
	out := &Volume{
		ID:           v.Id,
		Name:         v.Name,
		Region:       v.RegionId,
		SizeGB:       v.SizeGb,
		ActualSizeGB: deref(v.ActualSizeGb),
		Status:       v.Status,
		DesiredState: v.DesiredState,
		DeviceSerial: v.DeviceSerial,
		ErrorDetail:  deref(v.ErrorDetail),
	}
	if v.Attachment != nil {
		out.Attachment = &Attachment{
			VMID:      v.Attachment.VmId,
			Status:    v.Attachment.Status,
			TargetDev: deref(v.Attachment.TargetDev),
		}
	}
	return out
}

// needVolume — успешный статус обязан нести разобранный том.
//
// Сгенерированный клиент разбирает тело только при application/json, поэтому
// «2xx, но JSON не разобран» физически возможен — и тихо превращался бы в
// (nil, nil), то есть в успех без результата. Вызывающий падает на этом ниже
// по стеку, где причину уже не видно (поймано тестом CSI: панику давал
// vol.ID после Create).
func needVolume(v *api.V1Volume, resp *http.Response) (*Volume, error) {
	if v == nil {
		code := 0
		ctype := ""
		if resp != nil {
			code = resp.StatusCode
			ctype = resp.Header.Get("Content-Type")
		}
		return nil, fmt.Errorf("tatnet api: ответ %d не содержит тела тома (content-type %q)", code, ctype)
	}
	return toVolume(v), nil
}

// check превращает «не 2xx» в APIError. Сгенерированный клиент сам по статусу
// не ошибается — он отдаёт разобранное тело и HTTP-ответ, а решение о том, что
// считать отказом, оставляет вызывающему. Для CSI это решение несущее: 404 и
// 409 в части операций означают «нечего делать», то есть успех.
func check(resp *http.Response, body []byte) error {
	if resp == nil {
		return errors.New("tatnet api: пустой ответ")
	}
	if resp.StatusCode >= 300 {
		return &APIError{Status: resp.StatusCode, Body: string(body)}
	}
	return nil
}

// FindByName ищет том по имени. На этом стоит идемпотентность CreateVolume:
// CSI ретраится по построению, и без поиска повтор завёл бы второй LV, о
// котором никто не знает.
func (c *Client) FindByName(ctx context.Context, name string) (*Volume, error) {
	const page = 200
	// Постранично: у проекта может быть больше томов, чем помещается на
	// страницу, и «не нашёл на первой» — не то же самое, что «нет».
	for offset := 0; ; offset += page {
		limit, off := page, offset
		resp, err := c.gen.VolumesListVolumesWithResponse(ctx, c.projectID,
			&api.VolumesListVolumesParams{Limit: &limit, Offset: &off})
		if err != nil {
			return nil, err
		}
		if err := check(resp.HTTPResponse, resp.Body); err != nil {
			return nil, err
		}
		if resp.JSON200 == nil {
			return nil, errors.New("tatnet api: пустой список томов")
		}
		for i := range resp.JSON200.Data {
			if resp.JSON200.Data[i].Name == name {
				return toVolume(&resp.JSON200.Data[i]), nil
			}
		}
		if len(resp.JSON200.Data) < page {
			return nil, nil
		}
	}
}

func (c *Client) Get(ctx context.Context, id string) (*Volume, error) {
	resp, err := c.gen.VolumesGetVolumeWithResponse(ctx, c.projectID, id)
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	return needVolume(resp.JSON200, resp.HTTPResponse)
}

// Create заводит том. `k8s_cluster_id` едет всегда, когда он известен: он
// делает том собственностью кластера (`managed_by='csi'`) и он же — то, на чём
// держится авторизация, потому что ключ драйвера имеет скоуп
// `k8s_cluster:<id>`. Регион api выведет из кластера сам; шлём его для
// совместимости с ключами без кластерного скоупа.
func (c *Client) Create(ctx context.Context, name, regionID string, sizeGB int) (*Volume, error) {
	body := api.VolumesCreateVolumeJSONRequestBody{Name: name, SizeGb: sizeGB}
	if regionID != "" {
		body.RegionId = &regionID
	}
	if c.k8sClusterID != "" {
		body.K8sClusterId = &c.k8sClusterID
	}
	resp, err := c.gen.VolumesCreateVolumeWithResponse(ctx, c.projectID, body)
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	return needVolume(resp.JSON201, resp.HTTPResponse)
}

// Delete идемпотентен: уже удалённый том (404) — успех, потому что DeleteVolume
// в CSI обязан быть идемпотентным.
func (c *Client) Delete(ctx context.Context, id string) error {
	resp, err := c.gen.VolumesDeleteVolumeWithResponse(ctx, c.projectID, id)
	if err != nil {
		return err
	}
	if err := check(resp.HTTPResponse, resp.Body); IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func (c *Client) Attach(ctx context.Context, id, vmID string) (*Volume, error) {
	resp, err := c.gen.VolumesAttachVolumeWithResponse(ctx, c.projectID, id,
		api.VolumesAttachVolumeJSONRequestBody{VmId: vmID})
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	return needVolume(resp.JSON200, resp.HTTPResponse)
}

// Detach передаёт force=true как СВИДЕТЕЛЬСТВО, а не как «сломай».
//
// Гипервизор не спрашивает гостя: отключение живого virtio-диска со
// смонтированной ФС проходит успешно и молча (замерено на cluster1 21.08 —
// 123 мс, ни одного сообщения ядра). Поэтому платформа отказывает в откреплении
// работающей машины без явного флага. Kubernetes даёт нужную гарантию сам:
// attach/detach-контроллер зовёт NodeUnstageVolume до ControllerUnpublishVolume,
// то есть к этому моменту том размонтирован ЭТИМ же драйвером.
func (c *Client) Detach(ctx context.Context, id string) error {
	force := true
	resp, err := c.gen.VolumesDetachVolumeWithResponse(ctx, c.projectID, id,
		api.VolumesDetachVolumeJSONRequestBody{Force: &force})
	if err != nil {
		return err
	}
	err = check(resp.HTTPResponse, resp.Body)
	// 404 — тома нет; 409 — он не прикреплён. И то и другое означает
	// «прикрепления больше нет», а это и есть цель вызова.
	if IsNotFound(err) || IsConflict(err) {
		return nil
	}
	return err
}

func (c *Client) Resize(ctx context.Context, id string, sizeGB int) (*Volume, error) {
	resp, err := c.gen.VolumesResizeVolumeWithResponse(ctx, c.projectID, id,
		api.VolumesResizeVolumeJSONRequestBody{SizeGb: sizeGB})
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	return needVolume(resp.JSON200, resp.HTTPResponse)
}

// APIError несёт HTTP-статус, чтобы вызывающий отличал 404 и 409 от настоящего
// отказа: в CSI разница между «нечего делать» и «не смог» — это разница между
// идемпотентным успехом и вечным ретраем.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("tatnet api: status %d: %s", e.Status, strings.TrimSpace(e.Body))
}

func IsNotFound(err error) bool { return statusIs(err, http.StatusNotFound) }
func IsConflict(err error) bool { return statusIs(err, http.StatusConflict) }

func statusIs(err error, code int) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == code
}

// Node — нода кластера в связке с машиной TatNet.
type Node struct {
	VMID    string
	Address string
	Role    string
	Status  string
}

// Nodes отдаёт ноды кластера. Это ЗАПРОС, а не розданный заранее снимок:
// ноды добавляются и уезжают, и «какая машина стоит за этой нодой» обязано
// пересчитываться, иначе первый же добавленный узел получает отказ
// прикрепления, а уехавший — прикрепление в никуда.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	if c.k8sClusterID == "" {
		return nil, fmt.Errorf("кластер не задан — ноды не запросить")
	}
	resp, err := c.gen.KubernetesListClusterNodesWithResponse(ctx, c.projectID, c.k8sClusterID)
	if err != nil {
		return nil, err
	}
	if err := check(resp.HTTPResponse, resp.Body); err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, nil
	}
	out := make([]Node, 0, len(*resp.JSON200))
	for _, n := range *resp.JSON200 {
		out = append(out, Node{
			VMID:    deref(n.VmId),
			Address: deref(n.Address),
			Role:    n.Role,
			Status:  n.Status,
		})
	}
	return out, nil
}
