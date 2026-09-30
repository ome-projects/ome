import { parse } from 'yaml';
import type { PageStatus } from '../docs/types.ts';

export interface FrontMatter {
	title: string;
	navLabel: string | null;
	description: string;
	status: PageStatus | null;
	since: string | null;
	generated: boolean;
}

export interface ParsedPage {
	data: FrontMatter;
	body: string;
	/** 1-based line number of the first body line in the file. */
	bodyLine: number;
}

export const SINCE_PATTERN = /^v\d+\.\d+$/;

const KEYS = new Set(['title', 'navLabel', 'description', 'status', 'since', 'generated']);

/** Splits a page into validated front matter and its Markdown body. */
export function parseFrontMatter(source: string, path: string): ParsedPage {
	const text = source.replace(/\r\n?/g, '\n');
	const match = /^---\n([\s\S]*?)\n---(?:\n|$)/.exec(text);
	if (!match) {
		throw new Error(`${path}: missing front matter; start the file with a --- block`);
	}

	let raw: unknown;
	try {
		raw = parse(match[1]);
	} catch (error) {
		throw new Error(`${path}: invalid front matter: ${(error as Error).message}`, {
			cause: error
		});
	}
	if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
		throw new Error(`${path}: front matter must be a YAML mapping`);
	}

	const record = raw as Record<string, unknown>;
	for (const key of Object.keys(record)) {
		if (!KEYS.has(key)) throw new Error(`${path}: unknown front matter key "${key}"`);
	}

	const data: FrontMatter = {
		title: requiredString(record, 'title', path),
		navLabel: optionalString(record, 'navLabel', path),
		description: requiredString(record, 'description', path),
		status: status(record.status, path),
		since: since(record.since, path),
		generated: generated(record.generated, path)
	};

	return {
		data,
		body: text.slice(match[0].length),
		bodyLine: match[0].split('\n').length
	};
}

function requiredString(record: Record<string, unknown>, key: string, path: string): string {
	const value = record[key];
	if (value === undefined) throw new Error(`${path}: front matter needs "${key}"`);
	if (typeof value !== 'string' || value.trim() === '') {
		throw new Error(`${path}: front matter "${key}" must be a non-empty string`);
	}
	return value.trim();
}

function optionalString(record: Record<string, unknown>, key: string, path: string) {
	return record[key] === undefined ? null : requiredString(record, key, path);
}

function status(value: unknown, path: string): PageStatus | null {
	if (value === undefined) return null;
	if (value === 'draft' || value === 'preview') return value;
	throw new Error(`${path}: front matter "status" must be draft or preview`);
}

function since(value: unknown, path: string): string | null {
	if (value === undefined) return null;
	if (typeof value === 'string' && SINCE_PATTERN.test(value)) return value;
	throw new Error(`${path}: front matter "since" must look like v1.3`);
}

function generated(value: unknown, path: string): boolean {
	if (value === undefined) return false;
	if (typeof value === 'boolean') return value;
	throw new Error(`${path}: front matter "generated" must be true or false`);
}
