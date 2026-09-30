import { writable } from 'svelte/store';

export const heroShaderActive = writable(false);
export const heroShaderEnergy = writable(0);

export function toggleHeroShader() {
	heroShaderActive.update((value) => !value);
}
