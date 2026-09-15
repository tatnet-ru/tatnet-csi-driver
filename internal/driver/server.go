package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/tatnet-ru/tatnet-csi-driver/internal/tatnet"
)

// Mode — какой из сервисов CSI поднимает этот процесс.
type Mode string

const (
	ModeController Mode = "controller"
	ModeNode       Mode = "node"
)

// Server — gRPC-сервер CSI на unix-сокете.
type Server struct {
	grpc *grpc.Server
	addr string
}

// New собирает сервер под нужный режим.
//
// Identity поднимается всегда: kubelet и сайдкары спрашивают его первым, и
// процесс без него выглядит мёртвым независимо от того, что он умеет.
func New(mode Mode, cfg Config) (*Server, error) {
	srv := grpc.NewServer(grpc.UnaryInterceptor(logInterceptor))

	switch mode {
	case ModeController:
		if cfg.APIBase == "" || cfg.ProjectID == "" || cfg.APIKey == "" || cfg.RegionID == "" {
			return nil, fmt.Errorf("режиму controller нужны api-base, project-id, api-key и region-id")
		}
		api, err := tatnet.NewClient(cfg.APIBase, cfg.ProjectID, cfg.K8sClusterID, cfg.APIKey)
		if err != nil {
			return nil, err
		}
		csi.RegisterControllerServer(srv, &controllerServer{cfg: cfg, api: api})
		csi.RegisterIdentityServer(srv, &identityServer{ready: func() bool { return true }})
	case ModeNode:
		if cfg.NodeID == "" {
			return nil, fmt.Errorf("режиму node нужен идентификатор машины")
		}
		csi.RegisterNodeServer(srv, newNodeServer(cfg))
		csi.RegisterIdentityServer(srv, &identityServer{ready: func() bool { return true }})
	default:
		return nil, fmt.Errorf("неизвестный режим %q", mode)
	}

	return &Server{grpc: srv, addr: cfg.Endpoint}, nil
}

// Serve слушает unix-сокет до отмены контекста.
func (s *Server) Serve(ctx context.Context) error {
	path := strings.TrimPrefix(s.addr, "unix://")
	// Сокет остаётся на диске после падения процесса, и bind на существующий
	// путь — ошибка. Перезапуск обязан подниматься сам, без ручной уборки.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("убрать старый сокет %s: %w", path, err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("слушать %s: %w", path, err)
	}
	go func() {
		<-ctx.Done()
		s.grpc.GracefulStop()
	}()
	klog.InfoS("csi-драйвер слушает", "endpoint", s.addr, "version", Version)
	return s.grpc.Serve(lis)
}

// logInterceptor пишет каждый вызов и его исход.
//
// Полный запрос НЕ логируется: в CSI-вызовах едут secrets (учётные данные
// StorageClass), и «залогируем на всякий случай» — обычный способ разложить
// их по journald.
func logInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	resp, err := handler(ctx, req)
	if err != nil {
		klog.ErrorS(err, "csi вызов не удался", "method", info.FullMethod)
	} else {
		klog.V(4).InfoS("csi вызов", "method", info.FullMethod)
	}
	return resp, err
}
