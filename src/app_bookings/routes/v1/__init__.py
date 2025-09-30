"""V1 API routes."""

from fastapi import APIRouter

from src.routes.v1.healthcheck import router as healthcheck_router
from src.routes.v1.bookings import router as bookings_router

router = APIRouter()

# Include endpoint-specific routers
router.include_router(healthcheck_router)
router.include_router(bookings_router)