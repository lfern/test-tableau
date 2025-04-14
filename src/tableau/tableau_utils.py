import json
from tableauscraper import TableauScraper as TS, utils, TableauWorkbook
from tableauscraper.TableauScraper import TableauException
import re


class TableauScraper2(TS):
    def loads2(self, r):
        try:
            # dataReg = re.search(r"\d+;({.*})\d+;({.*})", r, re.MULTILINE)
            data_reg = re.search(r"\d+;({.*})\d+;({.*})", r, re.DOTALL)

            if data_reg is None:
                raise TableauException(message="Error parsing data")

            self.info = json.loads(data_reg.group(1))
            self.data = json.loads(data_reg.group(2))
            # self.dashboard_filter = self.getDashBoardFilter(self.info)

            if "presModelMap" in self.data["secondaryInfo"]:
                pres_model_map = self.data["secondaryInfo"]["presModelMap"]
                self.dataSegments = pres_model_map["dataDictionary"][
                    "presModelHolder"]["genDataDictionaryPresModel"]["dataSegments"]
                self.parameters = utils.getParameterControlInput(
                    self.info)
            self.dashboard = self.info["sheetName"]
            self.filters = utils.getFiltersForAllWorksheet(
                self.logger, self.data, self.info, rootDashboard=self.dashboard)
            self.filters2 = self.get_filters_for_all_worksheet2(self.data, self.info)
        except AttributeError:
            raise TableauException(message=r)

    def get_filters_for_all_worksheet2(self, data, info, cmd_response=False):
        filter_result = {}
        if cmd_response:
            pres_model = data["vqlCmdResponse"]["layoutStatus"]["applicationPresModel"]
            worksheets = utils.listWorksheetCmdResponse(pres_model)
            if len(worksheets) == 0:
                worksheets = utils.listStoryPointsCmdResponse(pres_model)
            for worksheet in worksheets:
                selected_filters = utils.getSelectedFilters(
                    pres_model,
                    worksheet["worksheet"]
                )
                filter_result[worksheet["worksheet"]] = selected_filters
        else:
            pres_model_map_viz_data = utils.getPresModelVizData(data)
            pres_model_map_viz_info = utils.getPresModelVizInfo(info)
            if pres_model_map_viz_data is not None:
                worksheets = utils.listWorksheet(pres_model_map_viz_data)
            elif pres_model_map_viz_info is not None:
                worksheets = utils.listWorksheetInfo(pres_model_map_viz_info)
                if len(worksheets) == 0:
                    worksheets = utils.listStoryPointsInfo(pres_model_map_viz_info)
            else:
                worksheets = []
            for worksheet in worksheets:
                selected_filters = utils.getSelectedFilters(
                    pres_model_map_viz_info, worksheet)
                filter_result[worksheet] = selected_filters
        return filter_result
