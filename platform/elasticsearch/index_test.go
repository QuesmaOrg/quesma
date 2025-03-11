// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import "testing"

func TestIsValidIndexName(t *testing.T) {

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "foo",
			wantErr: false,
		},
		{
			name:    "foo_bar",
			wantErr: false,
		},
		{
			name:    "esc_base_agent_client_cloud_container_data_stream_destination_device_dll_dns_ecs_email_error_event_faas_file_group_host_http_log_network_observer_orchestrator_organization_package_process_registry_related_rule_server_service_source_threat_tls_url_user_user_agent_volume_vulnerability_windows",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := IsValidIndexName(tt.name); (err != nil) != tt.wantErr {
				t.Errorf("IsValidIndexName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}
