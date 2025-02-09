"""Subclass of clsDBConfiguration, which is generated by wxFormBuilder."""

import wx
import threading

from yodatools.dataloader.view.clsDBConfig import clsDBConfiguration
from odm2api.ODMconnection import dbconnection as dbc

class frmDBConfig(wx.Dialog):
    def __init__(self, parent, service_manager, is_main=False):
        wx.Dialog.__init__(
            self,
            parent,
            title='Database Configuration',
            style=wx.DEFAULT_DIALOG_STYLE, size=wx.Size(500, 315)
        )
        self.panel = pnlDBConfig(self, service_manager, is_main)
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.AddWindow(self.panel, 1, border=1, flag=wx.EXPAND | wx.GROW | wx.ALL)  # noqa
        self.SetSizer(self.sizer)
        self.sizer.Fit(self.panel)
        self.connection_string = None


# Implementing clsDBConfiguration
class pnlDBConfig(clsDBConfiguration):
    def __init__(self, parent,  is_main=False):
        clsDBConfiguration.__init__(self, parent)

        self.choices = {
            'Microsoft SQL Server': 'mssql',
            'MySQL': 'mysql',
            'PostgreSQL': 'postgresql',
            'SQLite': 'sqlite'
        }
        self.cbDatabaseType.AppendItems(self.choices.keys())

        self.parent = parent
        self.is_main = is_main


    def OnValueChanged(self, event):
        """

        :param event:
        :return:

        """
        # self.btnSave.Enable(False)

        try:
            curr_dict = self.getFieldValues()
            if self.conn_dict == curr_dict:
                # self.btnSave.Enable(True)
                pass
            else:
                self.btnTest.Enable(True)

        except:
            pass

    # Handlers for clsDBConfiguration events.
    def OnBtnTest(self, event):

        self.SetCursor(wx.Cursor(wx.CURSOR_WAIT))

        conn_dict = self.getFieldValues()
        if self.validateInput(conn_dict):
            # self.btnSave.Enable(True)
            self.conn_dict = conn_dict
            # self.connection_string = dbc.createConnection(self.conn_dict)
            self.btnTest.Enable(False)

        self.SetCursor(wx.Cursor(wx.CURSOR_DEFAULT))

    # def OnBtnSave(self, event):
    #
    #     # self.parent.EndModal(wx.ID_OK)
    #     raise NotImplementedError

    # def OnBtnCancel(self, event):
    #     self.parent.SetReturnCode(wx.ID_CANCEL)
    #     self.parent.EndModal(wx.ID_CANCEL)

    def validateInput(self, conn_dict):
        message = ''
        if conn_dict['engine'] == "sqlite":
            if not conn_dict['address']:
                message = 'Please fill out the file path field in order to proceed'
                wx.MessageBox(message, 'Database Connection', wx.OK | wx.ICON_EXCLAMATION)  # noqa
                return False
        else:
            # Check that everything has been filled out.
            if not all(x for x in conn_dict.values()):
                message = 'Please complete every field in order to proceed'
                wx.MessageBox(message, 'Database Connection', wx.OK | wx.ICON_EXCLAMATION)  # noqa
                return False

        try:
            wx.BusyCursor()

            self.connection_string = dbc.buildConnectionString(**conn_dict)
            if dbc.isValidConnection(self.connection_string):
                message = 'This connection is valid'
                wx.MessageBox(message, 'Test Connection', wx.OK)
            else:

                wx.MessageBox(message, 'Error Occurred', wx.OK | wx.ICON_ERROR)
                return False
        except Exception as e:
            msg ="This connection is invalid"
            print (msg + str(e))
            wx.MessageBox(msg, 'Error Occurred', wx.ICON_ERROR | wx.OK)  # noqa
            return False
            # wx.MessageBox(e.message, 'Error Occurred', wx.ICON_ERROR | wx.OK)

        return True

    # Returns a dictionary of the database values entered in the form
    def getFieldValues(self):
        conn_dict = {}

        conn_dict['engine'] = self.choices[self.cbDatabaseType.GetValue()]
        conn_dict['user'] = self.txtUser.GetValue()
        conn_dict['password'] = self.txtPass.GetValue()
        conn_dict['address'] = self.txtServer.GetValue()
        conn_dict['db'] = self.txtDBName.GetValue()
        # conn_dict['version'] = ''

        return conn_dict

