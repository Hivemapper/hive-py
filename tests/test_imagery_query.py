import os
import json
import shutil
import pytest
from datetime import datetime

from tests.fixtures.fixture_feature import test_feature
from imagery import query

@pytest.fixture
def feature_file():
    test_feature_dict = json.loads(test_feature)
    file_path = 'test_feature.json'
    with open(file_path, 'w') as f:
        json.dump(test_feature_dict, f)
    
    os.makedirs('.hivepy_cache', exist_ok=True)
    
    yield file_path
    
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists('.hivepy_cache'):
        shutil.rmtree('.hivepy_cache')

@pytest.mark.real_api
def test_query(feature_file):
    """Test for 'query'"""
    auth = os.environ.get('HIVE_PY_UNIT_TEST_AUTH') or os.environ.get('HM_DEV_TOKEN')
    if not auth:
        pytest.skip('HIVE_PY_UNIT_TEST_AUTH or HM_DEV_TOKEN environment variable is not set')
    
    start_date = datetime.strptime('2025-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2025-01-02', '%Y-%m-%d')
    
    frames = query(feature_file, start_date, end_date, 'output', auth, use_cache=False)
    assert len(frames) == 23
