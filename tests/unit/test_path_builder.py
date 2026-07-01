from src.common.path_builder import _join_path

def test_join_path():
    assert _join_path('abfss://x/base/', '/abc/') == 'abfss://x/base/abc/'
