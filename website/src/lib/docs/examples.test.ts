import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

// These tutorials expose the same files that the Go admission tests load.
// Keep the copyable YAML tied to those fixtures instead of validating a
// different example from the one the reader actually sees.
const workflows = [
	{
		fixture: 'omenative-http',
		page: 'guides/omenative/learn-omenative.md',
		files: ['namespace.yaml', 'servingruntime.yaml', 'inferenceservice.yaml']
	},
	{
		fixture: 'runtime-managed-qwen',
		page: 'guides/deploy-models/deploy-an-inferenceservice.md',
		files: ['namespace.yaml', 'servingruntime.yaml', 'inferenceservice.yaml']
	},
	{
		fixture: 'standalone-http',
		page: 'guides/omenative/run-a-standalone-replica.md',
		files: [
			'namespace.yaml',
			'inferencereplica.yaml',
			'service.yaml',
			'servingruntime.yaml',
			'runtime-ref-replica.yaml',
			'update-response.json'
		]
	},
	{
		fixture: 'omenative-http',
		page: 'guides/omenative/recover-a-failed-http-workload.md',
		files: ['recovery-fail.patch.json', 'recovery-fix.patch.json']
	},
	{
		fixture: 'multi-cluster-registration',
		page: 'guides/multi-cluster/register-a-workload-cluster.md',
		files: [
			'namespace.yaml',
			'serviceaccount.yaml',
			'control-plane-values.yaml',
			'member-values.yaml',
			'workloadcluster.yaml',
			'servingruntime.yaml',
			'inferenceservice.yaml'
		]
	}
];

describe.each(workflows)('$page fixtures', ({ fixture, page, files }) => {
	const markdown = readFileSync(new URL(`../content/${page}`, import.meta.url), 'utf8');

	it.each(files)('embeds the checked %s fixture exactly once', (filename) => {
		const path = fileURLToPath(
			new URL(`../../../../config/samples/docs/${fixture}/${filename}`, import.meta.url)
		);
		const expected = readFileSync(path, 'utf8').trim();
		const blocks = [
			...markdown.matchAll(/^```(?:yaml|json) title="([^"]+)"\r?\n([\s\S]*?)^```\s*$/gm)
		];
		const matching = blocks.filter((block) => block[1] === filename);
		expect(matching, `Expected one ${filename} block in ${page}`).toHaveLength(1);
		expect(matching[0][2].trim()).toBe(expected);
	});
});
