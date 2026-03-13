from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/activities', tags=['activities'])

_activities_purged_before: Optional[datetime] = None


def _parse_activity_timestamp(activity: dict) -> Optional[datetime]:
    raw_value = (
        activity.get('transaction_time')
        or activity.get('date')
        or activity.get('timestamp')
        or activity.get('settle_date')
    )
    if not raw_value:
        return None

    try:
        text = str(raw_value).replace('Z', '+00:00')
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_activity(activity: dict) -> dict:
    timestamp = (
        activity.get('transaction_time')
        or activity.get('date')
        or activity.get('timestamp')
        or activity.get('settle_date')
    )
    activity_type = activity.get('activity_type') or activity.get('type')
    symbol = activity.get('symbol')
    qty = activity.get('qty') or activity.get('cum_qty') or activity.get('leaves_qty')
    price = activity.get('price') or activity.get('net_amount')
    identifier = activity.get('id') or activity.get('activity_id') or activity.get('order_id')

    return {
        'id': identifier,
        'timestamp': timestamp,
        'activity_type': activity_type,
        'symbol': symbol,
        'qty': qty,
        'price': price,
        'side': activity.get('side'),
        'order_status': activity.get('order_status'),
    }


@router.get('')
def list_activities(limit: int = 100, apply_purge: bool = False):
    raw_activities = alpaca_service.list_account_activities(limit=limit)
    global _activities_purged_before

    activities = [
        _normalize_activity(item)
        for item in raw_activities
        if isinstance(item, dict)
    ]

    if apply_purge and _activities_purged_before is not None:
        filtered = []
        for activity in activities:
            timestamp = _parse_activity_timestamp({'timestamp': activity.get('timestamp')})
            if timestamp is None or timestamp > _activities_purged_before:
                filtered.append(activity)
        activities = filtered

    return {
        'activities': activities,
        'count': len(activities),
        'purged_before': _activities_purged_before.isoformat() if _activities_purged_before else None,
        'deletion_scope': 'local_app_view_only',
    }


@router.post('/purge')
def purge_activities():
    global _activities_purged_before
    _activities_purged_before = datetime.now(timezone.utc)
    return {
        'ok': True,
        'purged_before': _activities_purged_before.isoformat(),
        'message': 'Purge locale appliquée (les activités broker Alpaca ne sont pas supprimées).',
    }


@router.post('/purge/reset')
def reset_purge_activities():
    global _activities_purged_before
    _activities_purged_before = None
    return {
        'ok': True,
        'purged_before': None,
        'message': 'Filtre de purge locale retiré. Les activités Alpaca sont de nouveau visibles.',
    }
