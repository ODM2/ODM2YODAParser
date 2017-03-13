from DataLoader.View.WizardSummaryPageView import WizardSummaryPageView
from DataLoader.Model.Inputs.ExcelInput import ExcelInput

class WizardSummaryPageController(WizardSummaryPageView):
    def __init__(self, parent, panel, title):
        super(WizardSummaryPageController, self).__init__(panel)
        self.parent = parent
        self.title = title

    def run(self):
        input_file = self.parent.home_page.input_file_text_ctrl.GetValue()
        excel = ExcelInput(input_file)
        excel.parse()
