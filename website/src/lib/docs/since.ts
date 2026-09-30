const SINCE = /^v(\d+)\.(\d+)$/;
const RELEASE = /^v?(\d+)\.(\d+)/;

export type SinceState = 'unreleased' | 'released' | 'unknown';

/** Compares a `vMAJOR.MINOR` marker with the latest release tag, such as `v1.2.2`. */
export function sinceState(since: string, latestRelease: string | null): SinceState {
	const target = SINCE.exec(since);
	if (!target) throw new Error(`invalid since value "${since}"; use vMAJOR.MINOR`);
	const latest = latestRelease ? RELEASE.exec(latestRelease) : null;
	if (!latest) return 'unknown';
	const [major, minor] = [Number(target[1]), Number(target[2])];
	const [latestMajor, latestMinor] = [Number(latest[1]), Number(latest[2])];
	const newer = major > latestMajor || (major === latestMajor && minor > latestMinor);
	return newer ? 'unreleased' : 'released';
}

export function sinceLabel(since: string, latestRelease: string | null): string {
	switch (sinceState(since, latestRelease)) {
		case 'unreleased':
			return `Unreleased: coming in ${since}`;
		case 'released':
			return `New in ${since}`;
		default:
			return `Since ${since}`;
	}
}

const SINCE_BADGE = /<span class="doc-since" data-since="(v\d+\.\d+)">[^<]*<\/span>/g;

/**
 * Rewrites the heading badges the renderer emits, labeling each against the
 * latest release. Runs per request, so no redeploy is needed when a release
 * ships.
 */
export function applySinceLabels(html: string, latestRelease: string | null): string {
	return html.replace(SINCE_BADGE, (_match, since: string) => {
		const state = sinceState(since, latestRelease);
		const label = sinceLabel(since, latestRelease);
		return `<span class="doc-since doc-since--${state}" data-since="${since}">${label}</span>`;
	});
}
