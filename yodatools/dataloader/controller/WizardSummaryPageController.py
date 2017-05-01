from yodatools.dataloader.view.WizardSummaryPageView import WizardSummaryPageView
from yodatools.converter.Inputs.ExcelInput import ExcelInput
from yodatools.converter.Inputs.yamlInput import yamlInput
from yodatools.converter.Outputs.yamlOutput import yamlOutput
from yodatools.converter.Outputs.dbOutput import dbOutput
import threading


class WizardSummaryPageController(WizardSummaryPageView):

    # Static variables
    # thread = None

    def __init__(self, parent, panel, title):
        super(WizardSummaryPageController, self).__init__(panel)
        self.parent = parent
        self.title = title

    def run(self, selections, yoda_output_file_path=None):
        # WizardSummaryPageController.thread = threading.currentThread()

        input_file = self.parent.home_page.input_file_text_ctrl.GetValue()

        # Check if it is a yaml, or excel file
        file_type = verify_file_type(input_file)

        if file_type == 'invalid':  # Accept only excel and yaml files
            print "File extension isvalid or no file"
            return

        if file_type == 'excel':
            kwargs = {'gauge': self.gauge}
            excel = ExcelInput(input_file, **kwargs)
            excel.parse()
            session = excel.sendODM2Session()
        else:
            # Must be a yoda file
            yoda = yamlInput(input_file)
            yoda.parse(input_file)
            session = yoda.sendODM2Session()

        # Go through each checkbox
        if 'excel' in selections:
            print 'export to an excel file has not been implemented'

        # if 'yoda' in selections:
        if yoda_output_file_path is not None:
            # Get the directory to save the yaml output
            yaml = yamlOutput()
            yaml.save(session=session, file_path=yoda_output_file_path)

        if 'odm2' in selections:
            print 'export to odm2'
            """
            create connection string
            call dboutput and do same as yoda export and send in connection string as filepath
            """

        session.close_all()

        self.gauge.SetValue(100)
        self.parent.load_finished_execution()
        return


def verify_file_type(input_file):
    CONST_LEGAL_EXCEL_EXTENSIONS = ('xlsx', 'xlsm')

    if input_file.endswith(CONST_LEGAL_EXCEL_EXTENSIONS):
        file_type = 'excel'
    elif input_file.endswith('yml'):
        file_type = 'yaml'
    else:
        file_type = 'invalid'

    return file_type
