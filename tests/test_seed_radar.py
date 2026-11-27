import unittest
import pandas as pd
import numpy as np
from seed_radar.engine import features, attach_pit, candidates
from seed_radar.backtest import simulate, labels, detection_metrics, metrics

class RadarTests(unittest.TestCase):
    def setUp(self):
        dates=pd.bdate_range("2024-01-01",periods=100)
        self.market=pd.DataFrame(dict(date=dates,close=np.linspace(100,120,100),regime="bull"))
        self.bars=pd.DataFrame(dict(date=dates,ticker="TEST",name="synthetic",open=100.,high=101.,low=99.,close=100.,volume=1000000,limit_up=110.,limit_down=90.))
    def test_asof(self):
        event=pd.DataFrame([dict(ticker="TEST",available_at="2024-01-02T14:00:00+08:00",source="synthetic",eps_revision=.1)])
        x=attach_pit(self.bars,event,"f")
        self.assertTrue(pd.isna(x.loc[x.date==pd.Timestamp("2024-01-02"),"f_eps_revision"].iloc[0]))
        self.assertEqual(x.loc[x.date==pd.Timestamp("2024-01-03"),"f_eps_revision"].iloc[0],.1)
    def test_no_future(self):
        a=features(self.bars,self.market)
        b=features(self.bars.iloc[:80],self.market.iloc[:80])
        pd.testing.assert_frame_equal(a.iloc[:80].reset_index(drop=True),b.reset_index(drop=True))
    def test_missing_news(self):
        x=features(self.bars,self.market)
        self.assertTrue(x.catalyst.isna().all())
        self.assertTrue(x.combined.isna().all())
    def test_locked_limit_and_next_open(self):
        sig=self.bars.iloc[[0]][["date","ticker"]].assign(score=90,regime="bull")
        a,b,o,u=simulate(self.bars,sig,self.market.date,1,initial=2000000)
        self.assertEqual(a.iloc[0].entry,self.market.date.iloc[1])
        self.assertLess(a.iloc[0].ret,0)
        locked=self.bars.copy()
        locked.loc[1,"open"]=110.
        a,b,o,u=simulate(locked,sig,self.market.date,1,initial=2000000)
        self.assertEqual(len(a),0)
        self.assertEqual(o.iloc[0].status,"limit_up")
    def test_labels_censor(self):
        lab=labels(self.bars,self.market.date)
        self.assertEqual(int(lab.label_valid.sum()),95)
        self.assertTrue(lab.lead.isna().all())

if __name__=="__main__": unittest.main()
