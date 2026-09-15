module github.com/tatnet-ru/tatnet-csi-driver

go 1.27.0

require (
	github.com/container-storage-interface/spec v1.11.0
	github.com/tatnet-ru/tatnet-go v0.2.1
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.11
	k8s.io/klog/v2 v2.130.1
	k8s.io/mount-utils v0.33.0
	k8s.io/utils v0.0.0-20260707023825-cf1189d6abe3
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/oapi-codegen/runtime v1.7.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
