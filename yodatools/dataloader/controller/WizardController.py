import threading

from WizardDatabasePageController import WizardDatabasePageController
from WizardExcelPageController import WizardExcelPageController
from WizardHomePageController import WizardHomePageController
from WizardSummaryPageController import WizardSummaryPageController
from WizardYodaPageController import WizardYodaPageController
from WizardSQLitePageController import WizardSQLitePageController

import wx

from yodatools.dataloader.view.WizardView import WizardView


class WizardController(WizardView):
    def __init__(self, parent):
        super(WizardController, self).__init__(parent)
        self.parent = parent
        self.yoda_page = WizardYodaPageController(self.body_panel, title='Yoda')  # noqa
        self.excel_page = WizardExcelPageController(self.body_panel, title='Excel')  # noqa
        self.database_page = WizardDatabasePageController(self.body_panel, title='ODM2')  # noqa
        self.sqlite_page = WizardSQLitePageController(self.body_panel, title='SQLite')  # noqa
        self.summary_page = WizardSummaryPageController(self, self.body_panel, title='Summary')  # noqa
        self.home_page = WizardHomePageController(self.body_panel, title='Loader Wizard')  # noqa
        self.execution_finished = False

        self.is_on_page_before_summary = False
        self.thread = threading.Thread()

        # The key must match the checkbox id
        self.home_page.pages_enabled = {
            0: True,   # home page
            1: False,  # yoda page
            2: False,  # excel page
            3: False,  # database page
            4: False,  # sqlite page
            5: True    # summary page
        }

        self.add_page(self.home_page)
        self.add_page(self.yoda_page)
        self.add_page(self.excel_page)
        self.add_page(self.database_page)
        self.add_page(self.sqlite_page)
        self.add_page(self.summary_page)
        self.next_button.Disable()

        self.show_home_page()
        self.SetSize((450, 450))

    def display_warning(self):
        """
        The yes/no are reversed to keep the exit on the left
         and cancel on the right

        :return:

        """

        dialog = wx.MessageDialog(
            self,
            message='It is unsafe to exit while a process is running.',
            style=wx.YES_NO | wx.ICON_EXCLAMATION
        )

        dialog.SetYesNoLabels(yes='Cancel', no='Exit')
        if dialog.ShowModal() == wx.ID_NO:
            self.Destroy()
        dialog.Destroy()

    def on_next_button(self, event):
        if self.execution_finished:
            self.Destroy()
            return

        if self.page_number + 2 > len(self.wizard_pages):
            if self.thread.isAlive():
                self.display_warning()
                return
            else:
                self.Destroy()

        self.wizard_pages[self.page_number].Hide()

        # Boundary checking
        self.page_number = self.__go_to_next_available_page(forward=True)
        self.__check_if_on_page_before_summary()

        self.__update_page()

    def __check_if_on_page_before_summary(self):
        self.is_on_page_before_summary = True
        for i in range(self.page_number + 1, len(self.home_page.pages_enabled.values()) - 1):  # noqa
            if self.home_page.pages_enabled[i]:
                self.is_on_page_before_summary = False

    def __go_to_next_available_page(self, forward=True):
        if forward:
            for i in range(self.page_number, len(self.home_page.pages_enabled.values())):  # noqa
                if self.home_page.pages_enabled[i] and i != self.page_number:
                    return i
        else:
            for i in range(self.page_number, -1, -1):
                if self.home_page.pages_enabled[i] and i != self.page_number:
                    return i

        return 0

    def on_back_button(self, event):
        self.wizard_pages[self.page_number].Hide()

        # Boundary checking
        self.page_number = self.__go_to_next_available_page(forward=False)
        self.__check_if_on_page_before_summary()

        self.__update_page()

    def __update_page(self):
        self.title_text.SetLabel(self.wizard_pages[self.page_number].title)

        if self.page_number == 0:
            self.will_flip_to_first_page()
        elif self.page_number == len(self.wizard_pages) - 1:
            self.will_flip_to_last_page()
            self.execute()  # Parse and save
        elif self.is_on_page_before_summary:
            self.will_flip_to_page_before_summary()
        else:
            self.next_button.SetLabel('Next')
            self.back_button.Show()

        self.wizard_pages[self.page_number].Show()
        self.body_panel.Layout()
        self.footer_panel.Layout()

    def will_flip_to_first_page(self):
        self.next_button.SetLabel('Next')
        self.back_button.Hide()

    def will_flip_to_last_page(self):
        self.next_button.SetLabel('Close')
        self.back_button.Show()

    def load_finished_execution(self):
        self.execution_finished = True
        self.next_button.SetLabel('Done')
        self.back_button.Hide()

    def will_flip_to_page_before_summary(self):
        self.next_button.SetLabel('Finish')
        self.back_button.SetLabel('Back')
        self.back_button.Show()

    def show_home_page(self):
        for page in self.wizard_pages:
            page.Hide()

        self.page_number = 0
        self.__update_page()

    def selected_pages(self):
        pages = {}
        if self.home_page.pages_enabled[1]:
            pages['yoda'] = self.yoda_page
        if self.home_page.pages_enabled[2]:
            pages['excel'] = self.excel_page
        if self.home_page.pages_enabled[3]:
            pages['odm2'] = self.database_page
        if self.home_page.pages_enabled[4]:
            pages['sqlite'] = self.sqlite_page

        return pages

    def execute(self):
        # Prevent the thread from  being created twice!
        if self.thread.isAlive():
            print('did not start another thread')
            return

        input_file = self.home_page.input_file_text_ctrl.GetValue()

        yoda_output_file_path = sqlite_conn = odm2_conn = None

        # Get the directory to save the yaml output
        sp = self.selected_pages()
        if sp.has_key('yoda'):
            yoda_page = self.selected_pages()['yoda']
            file_path = yoda_page.file_text_ctrl.GetValue()
            yoda_output_file_path = None if file_path == '' else file_path

        # Get the engine to save the ODM2 output
        if sp.has_key('odm2'):
            database_page = self.selected_pages()['odm2']
            conn_str = database_page.panel.connection_string
            odm2_conn = None if conn_str == '' else conn_str

        # Get the directory to save the sqlite output
        if sp.has_key('sqlite'):
            sqlite_page = self.selected_pages()['sqlite']
            conn_str = sqlite_page.panel.connection_string
            sqlite_conn = None if conn_str == '' else conn_str

        ##################################
        # Uncomment the lines below to have it threading
        ##################################
        # # Must be a tuple not a list
        # summary_run_arguments = (input_file, yoda_output_file_path)
        #
        # self.thread = threading.Thread(
        #     target=self.summary_page.run,
        #     args=summary_run_arguments,
        #     name='execution_thread'
        # )
        #
        # # When true, the thread will terminate when app is closed
        # # When false, the thread will continue even after the ap is closed
        # self.thread.setDaemon(True)
        # self.thread.start()

        ##################################
        # If you uncomment the lines above then you need to comment out
        # the line below
        ##################################

        self.summary_page.run(input_file, yoda_output_file_path, odm2_conn, sqlite_conn)
