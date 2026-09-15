// Команда tatnet-csi-driver — CSI-драйвер блочных томов TatNet.
//
// Один бинарь, два режима. Контроллер держит ключ /v1 и говорит с платформой;
// node-плагин работает с устройством и кредов не имеет вовсе — он крутится на
// каждой ноде кластера, включая те, куда попадает клиентская нагрузка.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/klog/v2"

	"github.com/tatnet-ru/tatnet-csi-driver/internal/driver"
)

func main() {
	klog.InitFlags(nil)

	var (
		mode       = flag.String("mode", "", "controller | node")
		endpoint   = flag.String("endpoint", "unix:///csi/csi.sock", "адрес CSI-сокета")
		regionID = flag.String("region-id", os.Getenv("TATNET_REGION_ID"), "регион кластера")
	)
	flag.Parse()

	cfg := driver.Config{
		Endpoint:  *endpoint,
		RegionID:  *regionID,
		APIBase:   os.Getenv("TATNET_API_BASE"),
		ProjectID: os.Getenv("TATNET_PROJECT_ID"),
		// Ключ приходит переменной окружения из смонтированного Secret'а и
		// НИКОГДА не флагом: флаги видны в /proc/*/cmdline любому на ноде.
		APIKey:       os.Getenv("TATNET_API_KEY"),
		K8sClusterID: os.Getenv("TATNET_K8S_CLUSTER_ID"),
	}

	if driver.Mode(*mode) == driver.ModeNode {
		// Нода зовётся своим АДРЕСОМ (InternalIP, downward API `status.hostIP`),
		// а машину за ним контроллер резолвит запросом к платформе. Идентичность
		// машины на ноду не доставляется вовсе: у ноды Kubernetes и машины
		// TatNet нет общего имени, а файл в машинном конфиге Talos не принимает
		// в immediate-режиме — попытка доставить его так роняет конвергенцию
		// конфига живой ноды, то есть перестают ехать и все прочие аддоны.
		cfg.NodeID = os.Getenv("TATNET_NODE_IP")
		if cfg.NodeID == "" {
			// Без адреса node-плагин бесполезен: том прикреплять не к чему.
			// Падаем сразу, а не притворяемся живыми.
			klog.ErrorS(nil, "TATNET_NODE_IP пуст — узел не опознать")
			os.Exit(1)
		}
	}

	srv, err := driver.New(driver.Mode(*mode), cfg)
	if err != nil {
		klog.ErrorS(err, "не удалось собрать драйвер")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		klog.ErrorS(err, "драйвер остановлен с ошибкой")
		os.Exit(1)
	}
}
