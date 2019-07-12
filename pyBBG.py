# pyBBG class for excel/R type access to blpapi

import blpapi
import datetime
import pandas as pd
import numpy as np


class pyBBG():

    def __init__(self):
        # establish session options
        self.sessionOptions = blpapi.SessionOptions()
        self.sessionOptions.setServerHost("localhost")
        self.sessionOptions.setServerPort(8194)

        # create Session
        self.session = blpapi.Session(self.sessionOptions)
        if not self.session.start():
            print("failed to start session: goto line 20")

        if not self.session.openService("//blp/refdata"):
            print("Failed to open refdata Service: line 24")

        def bar(self, sec, s_date, e_date, evtType="TRADE", intvl=60):

            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("IntradayBarRequest")
            request.set("security", sec)
            request.set("eventType", evtType)
            request.set("interval", intvl)
            request.set("startDateTime", s_dt)
            request.set("endDateTime", e_dt)
            self.session.sendRequest(request)

            while(True):
                ev = self.session.nextEvent(500)
                if ev.eventType() == blpapi.Event.RESPONSE:
                    a = [msg for msg in ev]
                    break

            pd_barData = pd.DataFrame(columns = ["time", "open", "high", "low", "close", "volume", "vwap"])
            bt_data = a[0].getElement(blpapi.Name("barData")).getElement(blpapi.Name("barTickData"))

            for bar in bt_data.values():
                t = bar.getElementAsDatetime("time")
                o = bar.getElementAsFloat("open")
                h = bar.getElementAsFloat("high")
                l = bar.getElementAsFloat("low")
                c = bar.getElementAsFloat("close")
                v = bar.getElementAsInteger("volume")
                if v != 0:
                    vwap = bar.getElementAsFloat("value")/v
                else:
                    vwap = bar.getElementAsFloat("value")/4

                pd_barData = pd_barData.append({"time":t,"open":o,"high":h,"low":l,"close":c,"volume":v,"vwap":vwap}, ignore_index=True)
            return(pd_barData)


        def bdp(self, sec=["TCEHY US EQUITY"], fields = ["LAST_PRICE"], overrides_fld = None, overides_val = None):
            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("ReferenceDataRequest")

            if (overrides_fld is not None) and (len(overrides_fld) != len(overrides_val)):
                print("error in length of overrides fields and values")

            if overrides_fld is not None:
                overrides = request.getElement("overrides")
                for f,v in zip(overrides_fld, overrides_val):
                    override1 = overrides.appendElement()
                    override1.setElement("fieldId", f)
                    override1.setElement("value", v)

            self.session.sendRequest(request)

            a = []
            while(True):
                ev = self.session.nextEvent(500)
                if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    print(ev.eventType())
                    a.append([msg for msg in ev][0])
                elif ev.eventType() == blpapi.Event.RESPONSE:
                    a.append([msg for msg in ev][0])
                    break

            if overrides_fld is None:
                data_df = pd.DataFrame(columns=fields)
                for i in a:
                    for n in i.getElement("securityData").values():
                        sec = n.getElement(blpapi.Name("security")).getValue()
                        sec_fld = {str(item.name()): item.getValue() for item in n.getElement(blpapi.Name("fieldData")).elements()}
                        data_df.ix[sec] = sec_fld
                return(data_df)

            else:
                x = a[0].getElement(blpapi.Name("securityData"))
                fld = [fld for fld in x.values()]
                sec_list = []
                sec_list = [fd.getElementAsString(blpapi.Name("security")) for fd in fld]

                field_dict = {}
                for field in fld[0].getElement(blpapi.Name("fieldData")).elements():
                    field_dict[str(field.name())] = field.getValue()

                first_pd = pd.DataFrame(field_dict, index=sec_list)

                if len(a) > 1:
                    for sd in a[1:]:
                        y = sd.getElement(blpapi.Name("securityData"))
                        fld = [fld for fld in y.values()]
                        sec = fld[0].getElementAsString(blpapi.Name("security"))

                        field_dict = {}
                        for field in fld[0].getElement(blpapi.Name("fieldData")).elements():
                            field_dict[str(field.name())] = field.getValue()

                        first_pd.ix[sec] = field_dict
                return(first_pd)

        def bdh(self, sec = ["TCEHY US EQUITY"], fields = ["LAST_PRICE"], s_dt = datetime.datetime.today()-datetime.timedelta(30), e_dt = datetime.datetime.today(), ovr_field_value=None):
            '''
            ovr_field_value = list of tuples [ ( field, value ) ]
            '''
            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("HistoricalDataRequest")

            [request.append("securities", s) for s in sec]

            if ovr_field_value:
                overrides = request.getElement("overrides")
                for elem in ovr_field_value:
                    overrides = overrides.appendElement()
                    overrides.setElement("fieldId", elem[0])
                    overrides.setElement("value", elem[1])

            request.set("startDate", s_dt.strftime("%Y%m%d"))
            request.set("endDate", e_dt.strftime("%Y%m%d"))
            self.session.sendRequest(request)

            a = []
            while(True):
                ev = self.session.nextEvent(500)
                if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    a.append([msg for msg in ev][0])
                else:
                    a.append([msg for msg in ev][0])
                    break

            data_out = pd.DataFrame()
            sec_list = []
            for o in a:
                i = o.getElement(blpapi.Name("securityData"))
                sec = i.getElement(blpapi.Name("security")).getValue()
                flddat = [f for f in i.getElement(blpapi.Name("fieldData")).values()]
                names = [[str(s.name()) for s in t.elements()] for t in flddat]
                vals = [[str(s.getValue()) for s in v.elements()] for v in flddat]
                sec_df = pd.DataFrame(vals, columns = names[0])
                if "date" not in data_out.columns.values:
                    data_out = pd.concat([data_out, sec_df], axis=1)
                else:
                    data_out = pd.merge(data_out, sec_df, on="date",how="outer")

                sec_list.append(sec)

            sec_list.insert(0, "date")
            data_out.columns = sec_list
            return(data_out)

        def tick(self, sec="700 HK EQUITY", s_dt = datetime.datetime.today()-datetime.timedelta(1)):

            start_end_time = self.bdp([sec], ["TRADING_DAY_START_TIME_EOD","TRADING_DAY_END_TIME_EOD"])
            today = s_dt.date()
            start_end_date = [today-datetime.timedelta(i.hour/12) for i in start_end_time.values[0]]
            s_e_values = [datetime.datetime.combine(start_end_date[i], start_end_time.values[0][i]) for i in range(len(start_end_date))]

            s_e_values = [x+datetime.timedelta(hours=5) for x in s_e_values]

            refDataService = self.session.getService("//blp/refdata")
            request = refDataService.createRequest("IntradayTickRequest")
            request.set("security", sec)
            request.getElement("eventTypes").appendValue("TRADE")
            request.getElement("eventTypes").appendValue("BID")
            request.getElement("eventTypes").appendValue("ASK")
            request.set("includeConditionCodes", True)

            request.set("startDateTime", s_e_values[0])
            request.set("endDateTime", s_e_values[1])

            self.session.sendRequest(request)

            a = []
            while(True):
                ev = self.session.nextEvent(500)
                if ev.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                    a.append([msg for msg in ev][0])
                else:
                    a.append([msg for msg in ev][0])
                    break

            tick_dict = {"Time":[], "Value":[], "Event":[], "Size":[]}

            def _process_ticks(elem):
                tick_dict["Event"].append(elem.getElement("type").getValueAsString())
                tick_dict["Value"].append(elem.getElement("value").getValueAsString())
                tick_dict["Time"].append(elem.getElement("time").getValueAsString())
                tick_dict["Size"].append(elem.getElement("size").getValueAsString())
                return(None)

            ticks = a[0].getElement("tickData").getElement("tickData").numValues()
            _ = [_process_ticks(a[0].getElement("tickData").getElement("tickData").getValue(i)) for i in range(ticks)]

            len_iter = range(len(tick_dict['Event']))
            bid_i = [i for i in len_iter if tick_dict["Event"][i] == 'BID']
            ask_i = [i for i in len_iter if tick_dict["Event"][i] == "ASK"]
            trd_i = [i for i in len_iter if tick_dict["Event"][i] == "TRADE"]

            def _series_maker(v_list, s_list, i_list, idx, name):
                dat = [v_list[i] for i in idx]
                siz = [s_list[i] for i in idx]
                size_str = "Size"+name[0]
                return(pd.DataFrame({name: dat, size_str: siz}, index=ix))

            tick_df = _series_maker(tick_dict['Value'], tick_dict['Size'], tick_dict['Time'], trd_i, "Trade")
            ask = _series_maker(tick_dict['Value'], tick_dict['Size'], tick_dict['Time'], ask_i, "Ask")
            bid = _series_maker(tick_dict['Value'], tick_dict['Size'], tick_dict['Time'], bid_i, "Bid")

            def df_combine(df_one, df_two, suffix=".a"):
                kp_idx = set(df_one.index.unique()) & set(df_two.index.unique())
                df_out = df_one.ix[kp_idx].join(df_two.ix[kp_idx], rsuffix = suffix)
                return(df_out)

            finalDF = df_combine(tick_df, ask, suffix=".a")
            finalDF = df_combine(finalDF, bid, suffix=".b")
            finalDF.ix[:, ["Ask","SizeA","Bid","SizeB"]] = finalDF.ix[:, ['Ask',"SizeA","Bid","SizeB"]].fillna(method='pad')
            finalDF = finalDF.dropna()

            return(finalDF)

        def cross_rate(self, sec, evtType, intvl, s_dt, e_dt):
            prx_cols = ["open","high","low","close","vwap"]
            adr_att = self.bdp(sec=[sec], fields = ["ADR_UNDL_CRNCY", "ADR_SH_PER_ADR", "ADR_UNDL_TICKER"])
            adr_ratio = adr_att["ADR_SH_PER_ADR"].get_values()[0]
            crncy = adr_att["ADR_UNDL_CRNCY"].get_values()[0]
            local_tick = adr_att["ADR_UNDL_TICKER"].get_values()[0]+" EQUITY"
            adj_factor = np.where(crncy[-1].islower(), 100, 1)
            crncy = crncy.upper() + "USD CURNCY"

            eur = self.bar(sec=crncy, s_dt=s_dt, e_dt=e_dt, evtType=evtType, intvl=intvl)
            adr = self.bar(sec=sec, s_dt=s_dt, e_dt=e_dt, evtType=evtType, intvl=intvl)
            loc = self.bar(sec=local_tick, s_dt=s_dt, e_dt=e_dt, evtType=evtType, intvl=intvl)
            loc.ix[:, prx_cols] = (loc.ix[:, prx_cols]/adj_factor)*adr_ratio

            eur["vwap"] = eur.ix[:,1:5].mean(axis=1)

            comb = loc.merge(eur, on="time", how="outer", suffixes=("_loc", "_fx")).fillna('pad')
            prx_loc = [w+"_loc" for w in prx_cols]
            prx_eur = [w+"_fx" for w in prx_cols]
            adj_loc = comb.ix[:, prx_loc].multiply(comb.ix[:, prx_eur].values)
            adj_loc["time"] = comb["time"]

            mkt = adr.merge(adj_loc, on="time", how="outer")

            spread = mkt.ix[:, ['open', 'close', 'vwap']]-mkt.ix[:, ['open_loc', 'close_loc', 'vwap']].values
            return(spread)

if __name__ == "__main__":
    print("success")
    
