package tenantctx

import "context"

func WithTenant(ctx context.Context, tenantId string) context.Context {
	return context.WithValue(ctx, tenantKey, tenantId)
}

func GetTenant(ctx context.Context) string {
	tenantId, _ := ctx.Value(tenantKey).(string)
	return tenantId
}

func MustGetTenant(ctx context.Context) string {
	tenantId := GetTenant(ctx)
	if tenantId == "" {
		panic("no tenant in context")
	}
	return tenantId
}

type tenantKeyType int

const tenantKey tenantKeyType = iota
