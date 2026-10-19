package operations

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
)

// These immutable registered materials identify the historical Video drift recovery.
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

func videoParallelObservedSource() releasebinding.Binding {
	return releasebinding.Binding{
		ReleaseRef:     "video-mcp-v0.17.35",
		AppID:          "video-mcp",
		AppVersion:     "0.17.35",
		PackageDigest:  "sha256:badeae0ce0ca1d1abeab20ffda1d15f98c28dafefc16e982d993459fa7a91340",
		ChartDigest:    "sha256:3bb879062371a320e890a69f92e3df5b8cae8ad07eb253aab9af6fa765f1db64",
		TemplateDigest: "sha256:6735cadfa908acb422fae9a04ff8a1e096f95487113bf69545bf4c8ce2650cd4",
	}
}

func videoFalEgressObservedSource() releasebinding.Binding {
	return releasebinding.Binding{
		ReleaseRef:     "video-mcp-v0.17.38",
		AppID:          "video-mcp",
		AppVersion:     "0.17.38",
		PackageDigest:  "sha256:e18c6689d681b6acade528aaa1b7fd382164ce0cfa9dff1884ecf41bd876858d",
		ChartDigest:    "sha256:71113d2193ba3e2722b6ce0149094881cf6f599e99f0c5ccded46b36e017b7ff",
		TemplateDigest: "sha256:edb4a393467e52dbfef30d8ceb2c5b0c09fca824c8f9c6d441ca5cbc7d469f56",
	}
}

// A registered source can lag the installed version after a completed managed
// transition. Only this exact historical chain authorizes the new adoption.
func videoInstalledSourceRecovery(ctx context.Context, tx pgx.Tx, installation, installed string, from, to releasebinding.Binding, action, state string) bool {
	if from.AppVersion == installed && videoSourceRecovery(from, to, action, state) {
		return true
	}
	if action != "adopt" || state != "installed" {
		return false
	}
	var sourceSnapshot, targetSnapshot string
	switch {
	case installed == "0.17.31" && from == videoObservedSource() && to == videoParallelObservedSource():
		sourceSnapshot, targetSnapshot = videoCredentialSourceSnapshot, videoCredentialTargetSnapshot
	case installed == "0.17.37" && from == videoParallelObservedSource() && to == videoFalEgressObservedSource():
		sourceSnapshot, targetSnapshot = videoProjectLabelsSourceSnapshot, videoProjectLabelsTargetSnapshot
	default:
		return false
	}
	var proven bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_installation_transitions
 WHERE installation_id=$1 AND app_id='video-mcp' AND action='upgrade' AND status='succeeded' AND phase='completed'
 AND source_snapshot=$2::jsonb AND target_snapshot=$3::jsonb)`, installation, sourceSnapshot, targetSnapshot).Scan(&proven)
	return err == nil && proven
}

const videoCredentialSourceSnapshot = `{"app_id":"video-mcp","app_version":"0.17.30","release_ref":"video-mcp-v0.17.30","release_digest":"sha256:62a455b0ac5dea6048f42dc1b7babd1d2b67d337ed9e09d8035ad7d9aeb93cb5","manifest_digest":"sha256:c9c4f4e266a93a937997845f412735e202d7ea494a117f0625721ec42259f593","template_digest":"sha256:2fb074bf0cdaead891827f59869974c23db3c93375e626e864c656b9bfe8cfd0","dashboard_digest":"sha256:08cb9f65c7733ab1ba43e3917aacda7a92a07934e3fc654fe9bfc354dfc5235c","data_contract":"","default_worker_version":"","compatible_worker_versions":[],"release_kind":"dashboard-release","app_image_digest":"sha256:4ad792a4756648008ecd479384d0f2c70c0ef20f437a15e1a030dc56fa9dba44"}`
const videoCredentialTargetSnapshot = `{"app_id":"video-mcp","app_version":"0.17.31","release_ref":"video-mcp-v0.17.31","release_digest":"sha256:d8f6d5876fdf396a94d4b0168180c0c3c1d96acc3463d2bb66f4761c3c2dc5b8","manifest_digest":"sha256:426cd81e411e2c7c34e6d0346ee09b9b53ae5b9aecc1db02c41a48145b84a1c6","template_digest":"sha256:56a832f56000efa1b0185f56187293a221aa151f0afd17a5e2baffc72253c991","dashboard_digest":"sha256:08cb9f65c7733ab1ba43e3917aacda7a92a07934e3fc654fe9bfc354dfc5235c","data_contract":"","default_worker_version":"","compatible_worker_versions":[],"release_kind":"dashboard-release","app_image_digest":"sha256:67ebb4aa1901ac5d4ef83206c301c68c7922a91e617408fdb51c9e4d098cbe33"}`
const videoProjectLabelsSourceSnapshot = `{"app_id":"video-mcp","app_version":"0.17.35","release_ref":"video-mcp-v0.17.35","release_digest":"sha256:da5cd14120338ba9f887f197aabac1cfa2c758fa1d88a917da7ae0b3fe96df64","manifest_digest":"sha256:56ac1cbfef890455b6c74098aeb2d48edb462f3bf94b3021a6f383688f9c7e18","template_digest":"sha256:6735cadfa908acb422fae9a04ff8a1e096f95487113bf69545bf4c8ce2650cd4","dashboard_digest":"sha256:e9c481418fcde62d8d10197ed12fa6f5949289a59beaed4b55ff1a580351d51d","data_contract":"","default_worker_version":"","compatible_worker_versions":[],"release_kind":"dashboard-release","app_image_digest":"sha256:8d053baf4ecc415c7468c73d84c2721ed5f20ea9076c9f9ad81543b14bb365b4"}`
const videoProjectLabelsTargetSnapshot = `{"app_id":"video-mcp","app_version":"0.17.37","release_ref":"video-mcp-v0.17.37","release_digest":"sha256:d24c6ab68831fb1c1c0ae31ab2407220eb5b331cf5c323662021238d4904199c","manifest_digest":"sha256:91ebf160aad2a827bad7a92b98371dde66fdf2b72f38b749672c6bfaf8474c85","template_digest":"sha256:9cfb65261291dfc9d12038acf4bd7e08b9aa16494a33249b5c884e4b8da90856","dashboard_digest":"sha256:09a8423ebae9c4cd9535bf4b76f901b18f7180c5a38af518d0c0ae22e3d18fc6","data_contract":"","default_worker_version":"","compatible_worker_versions":[],"release_kind":"dashboard-release","app_image_digest":"sha256:20a58cea21e5a1742bc4843eab7995848a03ad7c05f6f805611f7c8e1f5bc9c2"}`
