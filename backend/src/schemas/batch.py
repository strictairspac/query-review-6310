from pydantic import BaseModel,Field,model_validator
class BatchInput(BaseModel):
    name:str=Field(min_length=2,max_length=120)
    sample_id:int=Field(gt=0)
    candidate_sql:str=Field(min_length=6,max_length=6000)
    check_mode:str=Field(default='compare',pattern='^(compare|leaderboard)$')
    top_n:int|None=Field(default=None,ge=1,le=100)
    @model_validator(mode='after')
    def leaderboard_needs_top_n(self):
        if self.check_mode=='leaderboard' and not self.top_n: raise ValueError('榜单核对需指定取前几笔 top_n（1-100）')
        return self
