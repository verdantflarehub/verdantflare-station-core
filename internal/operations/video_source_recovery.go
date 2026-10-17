package operations

import "github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"

// These immutable signed materials identify the historical Video drift recovery.
// This is not an upgrade policy: Runtime must still verify the existing workload.
func videoSourceRecovery(from, to releasebinding.Binding, action, state string) bool {
	return action == "adopt" && state == "installed" && from == videoOriginalSource() && to == videoObservedSource()
}

func videoOriginalSource() releasebinding.Binding {
	return releasebinding.Binding{
		ReleaseRef: "video-mcp-v0.17.28", AppID: "video-mcp", AppVersion: "0.17.28",
		PackageDigest:  "sha256:a40746cc6d6b8870c9351fe7607524cd83d0d3f3036ddbf810aa5b550db6e5e9",
		ChartDigest:    "sha256:cd27e2099c8bb07d878c51e8d07674505b4c0bcc96bf8f3b52881b3150ce7f8a",
		TemplateDigest: "sha256:b078c549f50e88b521842556f9dd92c728d0e430973429f623d0d1d84c9696f7",
	}
}

func videoObservedSource() releasebinding.Binding {
	return releasebinding.Binding{
		ReleaseRef: "video-mcp-v0.17.30", AppID: "video-mcp", AppVersion: "0.17.30",
		PackageDigest:  "sha256:54668c7b6e57e57fb811240110550330fc931df80d3782fecf6d67e0d8f45a32",
		ChartDigest:    "sha256:bacdc4bfa638217075cec55097d86aac7fca374c7cd8733c9698ba0c65149113",
		TemplateDigest: "sha256:2fb074bf0cdaead891827f59869974c23db3c93375e626e864c656b9bfe8cfd0",
	}
}
