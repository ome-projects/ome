import type { ParamMatcher } from '@sveltejs/kit';
import { isSectionId } from '$lib/docs/paths';

export const match: ParamMatcher = (param) => isSectionId(param);
