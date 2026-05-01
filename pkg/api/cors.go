package api

import (
	"net/http"

	"github.com/go-chi/cors"
	"github.com/invergent-ai/surogate-hub/pkg/config"
)

func CORSMiddleware(next http.Handler, cfg config.Config) http.Handler {
	var origins []string
	if len(cfg.GetBaseConfig().CORS.AllowedOrigins) > 0 {
		origins = cfg.GetBaseConfig().CORS.AllowedOrigins
	}

	corshandler := cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Requested-With", "X-SgHub-Client", "X-Amz-Content-Sha256", "X-Amz-Date"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           365 * 24 * 3600, // 1 year
	})

	return corshandler(next)
}
