import type { SearchHit } from '$lib/docs/search-client';

export interface HitGroup {
	label: string;
	hits: SearchHit[];
}

/** Groups hits by section, in the order each section first appears. Hits keep their rank order. */
export function groupHits(hits: readonly SearchHit[]): HitGroup[] {
	const groups = new Map<string, SearchHit[]>();
	for (const hit of hits) {
		const group = groups.get(hit.sectionLabel);
		if (group) group.push(hit);
		else groups.set(hit.sectionLabel, [hit]);
	}
	return [...groups].map(([label, grouped]) => ({ label, hits: grouped }));
}

/** The hits in the order the groups show them, which is the order their rows are numbered in. */
export function rowOrder(groups: readonly HitGroup[]): SearchHit[] {
	return groups.flatMap((group) => group.hits);
}
