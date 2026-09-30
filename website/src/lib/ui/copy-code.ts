const RESET_MS = 1500;

/**
 * Copies a code block's text when its Copy button is clicked. One delegated
 * listener serves every block, so blocks rendered by client-side navigation
 * work without rebinding.
 */
export function copyCode(node: HTMLElement) {
	const timers = new Map<HTMLButtonElement, ReturnType<typeof setTimeout>>();
	// The buttons keep their "Copy to clipboard" name, so a status region reads the result.
	const status = document.createElement('p');
	status.className = 'visually-hidden';
	status.setAttribute('role', 'status');
	document.body.appendChild(status);

	async function onClick(event: MouseEvent) {
		if (!(event.target instanceof Element)) return;
		const button = event.target.closest<HTMLButtonElement>('.doc-code-copy');
		if (!button || !node.contains(button)) return;
		const code = button.closest('.doc-code')?.querySelector('pre code')?.textContent ?? '';
		let label = 'Copied';
		try {
			await navigator.clipboard.writeText(code);
		} catch {
			label = 'Failed';
		}
		button.textContent = label;
		status.textContent = label === 'Copied' ? 'Copied to clipboard' : 'Copy failed';
		clearTimeout(timers.get(button));
		timers.set(
			button,
			setTimeout(() => {
				button.textContent = 'Copy';
				status.textContent = '';
				timers.delete(button);
			}, RESET_MS)
		);
	}

	node.addEventListener('click', onClick);
	return {
		destroy() {
			node.removeEventListener('click', onClick);
			for (const timer of timers.values()) clearTimeout(timer);
			status.remove();
		}
	};
}
