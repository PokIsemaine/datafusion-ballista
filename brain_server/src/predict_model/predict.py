from dataclasses import dataclass
from proto.data_type import JobInfo


class StagePredictResult:
    """
    预测单个 Stage 的执行时间。
    """
    stage_id: int
    stage_execution_time_ms: float

@dataclass
class JobPredictResult:
    job_predict_time_ms: float
    stage_predict_results: list[StagePredictResult]
    
class PredictModel:
    def __init__(self, model_path: str = None):
        """
        初始化预测模型。如果 model_path 给出，可以加载已有模型。
        """
        self.model = None
        if model_path:
            self.load_model(model_path)
        else:
            self.mock_mode = True  # 没有模型时，自动启用 mock

    def load_model(self, model_path: str):
        """
        加载训练好的模型。
        """
        # TODO: 实际加载模型（比如用 joblib.load 或 torch.load 等）
        print(f"Loading model from {model_path}...")
        self.model = "LoadedModelPlaceholder"
        self.mock_mode = False

    def predict(self, job_info: JobInfo) -> JobPredictResult:
        """
        根据输入的 JobInfo（就是你刚刚解析出来的 dataclass）
        预测执行时间（单位 ms）。
        """
        if self.mock_mode:
            return self._mock_predict(job_info)
        else:
            return self._real_predict(job_info)

    def _real_predict(self, job_info: JobInfo) -> JobPredictResult:
        """
        用真正的模型进行预测。
        """
        # TODO: 提取 job_info 特征，送入 self.model 预测
        raise NotImplementedError("Real prediction not implemented yet.")

    def _mock_predict(self, job_info: JobInfo) -> JobPredictResult:
        """
        Mock 预测逻辑，用于开发阶段测试。
        """
        # 简单用 Stage 数量、Operator 数量决定一个 mock 预测时间
        num_stages = len(job_info.stages)
        num_operators = sum(len(stage.operators) for stage in job_info.stages)

        estimated_time_ms = 100 * num_stages + 10 * num_operators

        return JobPredictResult(execution_time_ms=estimated_time_ms)
