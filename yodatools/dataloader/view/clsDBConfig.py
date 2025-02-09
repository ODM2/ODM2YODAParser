# -*- coding: utf-8 -*-

###########################################################################
# Python code generated with wxFormBuilder (version Jun  5 2014)
# http://www.wxformbuilder.org/
#
# TODO: (Is this something we should follow?) PLEASE DO 'NOT' EDIT THIS FILE!
###########################################################################

import wx


###########################################################################
# Class clsDBConfiguration
###########################################################################

class clsDBConfiguration(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent, id=wx.ID_ANY,
                          pos=wx.DefaultPosition,
                          size=wx.Size(500, 291),
                          style=wx.SIMPLE_BORDER | wx.TAB_TRAVERSAL)

        self.SetMinSize(wx.Size(442, 291))
        self.SetMaxSize(wx.Size(627, 291))

        formSizer = wx.BoxSizer(wx.VERTICAL)

        sbSizer = wx.StaticBoxSizer(wx.StaticBox(self, wx.ID_ANY, 'Database Connection'), wx.VERTICAL)  # noqa

        connectionSizer = wx.FlexGridSizer(0, 2, 0, 15)
        connectionSizer.AddGrowableCol(1)
        connectionSizer.SetFlexibleDirection(wx.VERTICAL)
        connectionSizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_ALL)

        self.stVersion = wx.StaticText(self, wx.ID_ANY, 'DB Version:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stVersion.Wrap(-1)
        connectionSizer.Add(self.stVersion, 0, wx.ALL | wx.ALIGN_RIGHT | wx.EXPAND, 5)  # noqa

        cbDatabaseType1Choices = ['2.0']#, '1.1.1']
        self.cbDatabaseType1 = wx.ComboBox(self, wx.ID_ANY, '2.0', wx.DefaultPosition, wx.DefaultSize, cbDatabaseType1Choices, wx.CB_READONLY )  # noqa
        self.cbDatabaseType1.SetSelection(1)
        connectionSizer.Add(self.cbDatabaseType1, 1, wx.ALL | wx.EXPAND, 5)

        self.stConnType = wx.StaticText(self, wx.ID_ANY, 'Connection Type:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stConnType.Wrap(-1)
        connectionSizer.Add(self.stConnType, 0, wx.ALL | wx.EXPAND | wx.ALIGN_RIGHT, 5)  # noqa

        cbDatabaseTypeChoices = []
        self.cbDatabaseType = wx.ComboBox(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, cbDatabaseTypeChoices, wx.CB_READONLY)  # noqa
        connectionSizer.Add(self.cbDatabaseType, 1, wx.ALL | wx.EXPAND, 5)

        self.stServer = wx.StaticText(self, wx.ID_ANY, 'Server:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stServer.Wrap(-1)
        connectionSizer.Add(self.stServer, 0, wx.ALL | wx.EXPAND | wx.ALIGN_RIGHT, 5)  # noqa

        self.txtServer = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0 | wx.FULL_REPAINT_ON_RESIZE | wx.SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtServer, 1, wx.ALL | wx.EXPAND, 5)

        self.stDBName = wx.StaticText(self, wx.ID_ANY, 'Database:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stDBName.Wrap(-1)
        self.stDBName.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 90, False, wx.EmptyString))  # noqa

        connectionSizer.Add(self.stDBName, 0, wx.ALL | wx.EXPAND | wx.ALIGN_RIGHT, 5)  # noqa

        self.txtDBName = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0 | wx.SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtDBName, 1, wx.ALL | wx.EXPAND, 5)

        self.stUser = wx.StaticText(self, wx.ID_ANY, 'User:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stUser.Wrap(-1)
        self.stUser.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 90, False, wx.EmptyString))  # noqa

        connectionSizer.Add(self.stUser, 0, wx.ALL | wx.EXPAND | wx.ALIGN_RIGHT, 5)  # noqa

        self.txtUser = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, 0 | wx.SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtUser, 1, wx.ALL | wx.EXPAND, 5)

        self.stPass = wx.StaticText(self, wx.ID_ANY, 'Password:', wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_RIGHT)  # noqa
        self.stPass.Wrap(-1)
        self.stPass.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 90, False, wx.EmptyString))  # noqa

        connectionSizer.Add(self.stPass, 0, wx.ALL | wx.EXPAND | wx.ALIGN_RIGHT, 5)  # noqa

        self.txtPass = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, wx.DefaultPosition, wx.DefaultSize, wx.TE_PASSWORD | wx.SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtPass, 1, wx.ALL | wx.EXPAND, 5)

        sbSizer.Add(connectionSizer, 90, wx.EXPAND, 3)

        formSizer.Add(sbSizer, 1, wx.ALL | wx.EXPAND, 7)

        btnSizer = wx.FlexGridSizer(0, 3, 0, 25)
        btnSizer.AddGrowableCol(0)
        btnSizer.AddGrowableCol(1)
        btnSizer.AddGrowableCol(2)
        btnSizer.SetFlexibleDirection(wx.VERTICAL)
        btnSizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_ALL)


        self.btnTest = wx.Button(self, wx.ID_ANY, 'Test Connection', wx.DefaultPosition, wx.DefaultSize, 0)  # noqa
        btnSizer.Add(self.btnTest, 0, wx.ALL | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL | wx.ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        # self.btnSave = wx.Button(self, wx.ID_ANY, 'Save Connection', wx.DefaultPosition, wx.DefaultSize, 0)  # noqa
        # btnSizer.Add(self.btnSave, 0, wx.ALL | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL | wx.ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        # self.btnCancel = wx.Button(self, wx.ID_ANY, 'Cancel', wx.DefaultPosition, wx.DefaultSize, 0)  # noqa
        # btnSizer.Add(self.btnCancel, 0, wx.ALL | wx.EXPAND | wx.ALIGN_CENTER_VERTICAL | wx.ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        formSizer.Add(btnSizer, 10, wx.EXPAND, 2)

        self.SetSizer(formSizer)
        self.Layout()

        # Connect Events.
        self.btnTest.Bind(wx.EVT_BUTTON, self.OnBtnTest)
        # self.btnSave.Bind(wx.EVT_BUTTON, self.OnBtnSave)
        # self.btnCancel.Bind(wx.EVT_BUTTON, self.OnBtnCancel)
        self.btnSizer = btnSizer
        self.formSizer = formSizer

        self.btnTest.SetFocus()

    def __del__(self):
        pass

    # Virtual event handlers, overide them in your derived class.
    def OnBtnTest(self, event):
        event.Skip()

    # def OnBtnSave(self, event):
    #     event.Skip()
    #
    # def OnBtnCancel(self, event):
    #     event.Skip()
