import pandas as pd
import pytest

import d6tflow


@pytest.mark.parametrize('first_keys', ['customers', ['customers']])
@pytest.mark.parametrize('use_cache', [False, True])
def test_excel_cached_sheet_selection(tmp_path, monkeypatch, first_keys, use_cache):
    monkeypatch.setattr(d6tflow.settings, 'cached', use_cache)
    customers = pd.DataFrame({'customer': [1, 2]})
    orders = pd.DataFrame({'order': [3, 4]})

    class Sheets(d6tflow.tasks.TaskExcelPandas):
        persist = ['customers', 'orders']

        def run(self):
            self.save({'customers': customers, 'orders': orders})

    flow = d6tflow.Workflow(Sheets, path=tmp_path)
    flow.run()
    flow.outputLoad(keys=first_keys, cached=use_cache)
    all_sheets = flow.outputLoad(as_dict=True, cached=use_cache)
    assert set(all_sheets) == {'customers', 'orders'}
    pd.testing.assert_frame_equal(all_sheets['customers'], customers)
    pd.testing.assert_frame_equal(all_sheets['orders'], orders)
    pd.testing.assert_frame_equal(flow.outputLoad(keys='orders', cached=use_cache), orders)
    pd.testing.assert_frame_equal(flow.outputLoad(keys=['customers'], cached=use_cache)[0], customers)
