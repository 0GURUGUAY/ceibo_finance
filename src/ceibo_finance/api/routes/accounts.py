from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/account', tags=['account'])


@router.get('')
def get_account():
    account = alpaca_service.get_account()
    return account.model_dump() if hasattr(account, 'model_dump') else account
