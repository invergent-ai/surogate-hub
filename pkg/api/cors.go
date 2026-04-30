package api

import (
	"github.com/go-chi/cors"
	"github.com/invergent-ai/surogate-hub/pkg/config"
	"net/http"
)

func CORSMiddleware(next http.Handler, cfg config.Config) http.Handler {
	var origins []string
	if len(cfg.GetBaseConfig().CORS.AllowedOrigins) > 0 {
		origins = cfg.GetBaseConfig().CORS.AllowedOrigins
	} else {
		origins = []string{"https://*.densemax.local", "https://*.densemax.local:4200", "https://localhost:4200", "https://localhost:3000"}
	}

	corshandler := cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Requested-With", "X-Lakefs-Client", "X-Amz-Content-Sha256", "X-Amz-Date"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           365 * 24 * 3600, // 1 year
	})

	return corshandler(next)
}
