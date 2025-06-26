// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/exec"
	"time"
)

func NewManuallyCreatedContainer() *ManuallyCreatedContainer {
	return &ManuallyCreatedContainer{}
}

type ManuallyCreatedContainer struct{}

func (c ManuallyCreatedContainer) GetContainerID() string {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Endpoint(ctx context.Context, s string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) PortEndpoint(ctx context.Context, port nat.Port, s string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Host(ctx context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Inspect(ctx context.Context) (*types.ContainerJSON, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) MappedPort(ctx context.Context, port nat.Port) (nat.Port, error) {
	return nat.Port("8080/tcp"), nil
}

func (c ManuallyCreatedContainer) Ports(ctx context.Context) (nat.PortMap, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) SessionID() string {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) IsRunning() bool {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Start(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Stop(ctx context.Context, duration *time.Duration) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Terminate(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Logs(ctx context.Context) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) FollowOutput(consumer testcontainers.LogConsumer) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) StartLogProducer(ctx context.Context, option ...testcontainers.LogProductionOption) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) StopLogProducer() error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Name(ctx context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) State(ctx context.Context) (*types.ContainerState, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Networks(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) NetworkAliases(ctx context.Context) (map[string][]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) Exec(ctx context.Context, cmd []string, options ...exec.ProcessOption) (int, io.Reader, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) ContainerIP(ctx context.Context) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) ContainerIPs(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) CopyToContainer(ctx context.Context, fileContent []byte, containerFilePath string, fileMode int64) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) CopyDirToContainer(ctx context.Context, hostDirPath string, containerParentPath string, fileMode int64) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) CopyFileToContainer(ctx context.Context, hostFilePath string, containerFilePath string, fileMode int64) error {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) CopyFileFromContainer(ctx context.Context, filePath string) (io.ReadCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (c ManuallyCreatedContainer) GetLogProductionErrorChannel() <-chan error {
	//TODO implement me
	panic("implement me")
}
