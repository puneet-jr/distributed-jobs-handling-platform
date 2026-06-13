package handler
import (
	 "distributed-job-platform/internal/jobs/dto"
    "distributed-job-platform/internal/jobs/service"
    "net/http"

	"github.com/gin-gonic/gin"
)
 
type JobHandler struct {
	svc service.JobService
}

func NewJobHandler(svc service.JobService) *JobHandler {
	return &JobHandler{svc:svc}
}

func (h *JobHandler) Create(ctx *gin.Context) {
    var req dto.CreateJobRequest
    if err := ctx.ShouldBindJSON(&req); err != nil {
        ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

	job, err := h.svc.CreateJob(ctx.Request.Context(), req)
    if err != nil {
        ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    ctx.JSON(http.StatusCreated, job)
}

func(h *JobHandler) GetByID(ctx *gin.context) {
	id := ctx.Param("id")

	job,err := h.svc.GetJob(ctx.Request.Context(),id)

	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	ctx.JSON(http.StatusOK,job)
}

func(h *JobHandler) List(ctx *gin.Context) {
	jobs, err := h.svc.ListJobs(ctx.Request.Context())

	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error":err.Error()})
		return
	}
	ctx.JSON(http.StatusOK,jobs)
}

func(h *JobHandler) UpdateStatus(ctx *gin.Context) {
	id := ctx.Param("id")
	var req dto.UpdateStatusRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err:= h.svc.UpdateStatus(ctx.Request.Context(), id, req.Status); err!= nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
        return
	}
	ctx.JSON(http.StatusOK, gin.H{"message": "status updated"})
}
