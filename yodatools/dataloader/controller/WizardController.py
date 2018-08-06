import os
import threading

from WizardDatabasePageController import WizardDatabasePageController
from WizardExcelPageController import WizardExcelPageController
from WizardHomePageController import WizardHomePageController
from WizardSummaryPageController import WizardSummaryPageController
from WizardYodaPageController import WizardYodaPageController
from WizardSQLitePageController import WizardSQLitePageController

import wx
# from wx.lib.pubsub import pub

from yodatools.dataloader.view.WizardView import WizardView
from odm2api.ODMconnection import dbconnection as dbc


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

        # pub.subscribe(self.handleError, 'wizardcontroller.error')

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

        try:
            self.__update_page()
        except AttributeError:
            pass

    def __update_page(self):
        self.title_text.SetLabel(self.wizard_pages[self.page_number].title)

        if self.page_number == 0:
            self.will_flip_to_first_page()
        elif self.page_number == len(self.wizard_pages) - 1:
            self.will_flip_to_last_page()

            yoda_output_file_path = odm2_conn = sqlite_conn = None

            # Get the directory to save the yaml output
            sp = self.selected_pages()
            if 'yoda' in sp:
                yoda_page = self.selected_pages()['yoda']
                file_path = yoda_page.file_text_ctrl.GetValue()
                yoda_output_file_path = None if file_path == '' else file_path

            # Get the engine to save the ODM2 output
            if 'odm2' in sp:
                database_page = self.selected_pages()['odm2']
                conn_dict = database_page.panel.getFieldValues()
                odm2_conn = dbc.buildConnectionString(**conn_dict)

            # Get the directory to save the sqlite output
            if 'sqlite' in sp:
                sqlite_page = self.selected_pages()['sqlite']
                conn_dict = sqlite_page.panel.getFieldValues()
                sqlite_conn = dbc.buildConnectionString(**conn_dict)

            input_file = self.home_page.input_file_text_ctrl.GetValue()

            self.execute(input_file, yoda_output_file_path=yoda_output_file_path, odm2_conn=odm2_conn,
                         sqlite_conn=sqlite_conn)  # Parse and save

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

    def execute(self, input_file, yoda_output_file_path=None, odm2_conn=None, sqlite_conn=None):
        # Prevent the thread from  being created twice!
        if self.thread.isAlive():
            print('did not start another thread')
            return

        """
        Uncomment the line below to execute on a single thread
        """
        # self.summary_page.run(input_file, yoda_output_file_path, odm2_conn, sqlite_conn)

        """
        Uncomment the lines below to execute on background thread
        """
        self.thread = threading.Thread(
            target=self.summary_page.run,
            args=(input_file, yoda_output_file_path, odm2_conn, sqlite_conn),
            name='execution_thread'
        )

        # When true, the thread will terminate when app is closed
        # When false, the thread will continue even after the ap is closed
        self.thread.setDaemon(True)
        self.thread.start()

    def handleError(self, message):
        wx.MessageBox("An exception has occurred:\n\n%s" % message, style=wx.ICON_ERROR)
