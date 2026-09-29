package effective

// RenderSpec returns a deep copy of the component's merged spec: an
// *v1beta1.EngineSpec, *v1beta1.DecoderSpec, or *v1beta1.RouterSpec, or nil
// when the component carries none. The spec can hold literal credentials, so
// only kubectl ome runtime render, whose output is documented as unredacted,
// may print it. Report projections must copy allowlisted fields instead.
func (c EffectiveComponent) RenderSpec() any {
	switch {
	case c.engine != nil:
		return c.engine.DeepCopy()
	case c.decoder != nil:
		return c.decoder.DeepCopy()
	case c.router != nil:
		return c.router.DeepCopy()
	default:
		return nil
	}
}
