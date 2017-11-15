# -*- coding: utf-8 -*-

###########################################################################
# Python code generated with wxFormBuilder (version Jun  5 2014)
# http://www.wxformbuilder.org/
#
# TODO: (Is this something we should follow?) PLEASE DO 'NOT' EDIT THIS FILE!
###########################################################################

# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl,\
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, HORIZONTAL,\
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON, GROW




###########################################################################
# Class clsDBConfiguration
###########################################################################

class clsDBConfiguration(Panel):
    def __init__(self, parent):
        Panel.__init__(self, parent, id=ID_ANY,
                          pos=DefaultPosition,
                          size=Size(500, 291),
                          style=SIMPLE_BORDER | TAB_TRAVERSAL)

        self.SetMinSize(Size(442, 291))
        self.SetMaxSize(Size(627, 291))

        formSizer = BoxSizer(VERTICAL)

        sbSizer = StaticBoxSizer(StaticBox(self, ID_ANY, 'Database Connection'), VERTICAL)  # noqa

        connectionSizer = FlexGridSizer(0, 2, 0, 15)
        connectionSizer.AddGrowableCol(1)
        connectionSizer.SetFlexibleDirection(VERTICAL)
        connectionSizer.SetNonFlexibleGrowMode(FLEX_GROWMODE_ALL)

        self.stVersion = StaticText(self, ID_ANY, 'DB Version:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stVersion.Wrap(-1)
        connectionSizer.Add(self.stVersion, 0, ALL | ALIGN_RIGHT | EXPAND, 5)  # noqa

        cbDatabaseType1Choices = ['2.0']#, '1.1.1']
        self.cbDatabaseType1 = ComboBox(self, ID_ANY, '2.0', DefaultPosition, DefaultSize, cbDatabaseType1Choices, CB_READONLY )  # noqa
        self.cbDatabaseType1.SetSelection(1)
        connectionSizer.Add(self.cbDatabaseType1, 1, ALL | EXPAND, 5)

        self.stConnType = StaticText(self, ID_ANY, 'Connection Type:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stConnType.Wrap(-1)
        connectionSizer.Add(self.stConnType, 0, ALL | EXPAND | ALIGN_RIGHT, 5)  # noqa

        cbDatabaseTypeChoices = []
        self.cbDatabaseType = ComboBox(self, ID_ANY, EmptyString, DefaultPosition, DefaultSize, cbDatabaseTypeChoices, CB_READONLY)  # noqa
        connectionSizer.Add(self.cbDatabaseType, 1, ALL | EXPAND, 5)

        self.stServer = StaticText(self, ID_ANY, 'Server:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stServer.Wrap(-1)
        connectionSizer.Add(self.stServer, 0, ALL | EXPAND | ALIGN_RIGHT, 5)  # noqa

        self.txtServer = TextCtrl(self, ID_ANY, EmptyString, DefaultPosition, DefaultSize, 0 | FULL_REPAINT_ON_RESIZE | SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtServer, 1, ALL | EXPAND, 5)

        self.stDBName = StaticText(self, ID_ANY, 'Database:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stDBName.Wrap(-1)
        self.stDBName.SetFont(Font(NORMAL_FONT.GetPointSize(), 70, 90, 90, False, EmptyString))  # noqa

        connectionSizer.Add(self.stDBName, 0, ALL | EXPAND | ALIGN_RIGHT, 5)  # noqa

        self.txtDBName = TextCtrl(self, ID_ANY, EmptyString, DefaultPosition, DefaultSize, 0 | SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtDBName, 1, ALL | EXPAND, 5)

        self.stUser = StaticText(self, ID_ANY, 'User:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stUser.Wrap(-1)
        self.stUser.SetFont(Font(NORMAL_FONT.GetPointSize(), 70, 90, 90, False, EmptyString))  # noqa

        connectionSizer.Add(self.stUser, 0, ALL | EXPAND | ALIGN_RIGHT, 5)  # noqa

        self.txtUser = TextCtrl(self, ID_ANY, EmptyString, DefaultPosition, DefaultSize, 0 | SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtUser, 1, ALL | EXPAND, 5)

        self.stPass = StaticText(self, ID_ANY, 'Password:', DefaultPosition, DefaultSize, ALIGN_RIGHT)  # noqa
        self.stPass.Wrap(-1)
        self.stPass.SetFont(Font(NORMAL_FONT.GetPointSize(), 70, 90, 90, False, EmptyString))  # noqa

        connectionSizer.Add(self.stPass, 0, ALL | EXPAND | ALIGN_RIGHT, 5)  # noqa

        self.txtPass = TextCtrl(self, ID_ANY, EmptyString, DefaultPosition, DefaultSize, TE_PASSWORD | SIMPLE_BORDER)  # noqa
        connectionSizer.Add(self.txtPass, 1, ALL | EXPAND, 5)

        sbSizer.Add(connectionSizer, 90, EXPAND, 3)

        formSizer.Add(sbSizer, 1, ALL | EXPAND, 7)

        btnSizer = FlexGridSizer(0, 3, 0, 25)
        btnSizer.AddGrowableCol(0)
        btnSizer.AddGrowableCol(1)
        btnSizer.AddGrowableCol(2)
        btnSizer.SetFlexibleDirection(VERTICAL)
        btnSizer.SetNonFlexibleGrowMode(FLEX_GROWMODE_ALL)


        self.btnTest = Button(self, ID_ANY, 'Test Connection', DefaultPosition, DefaultSize, 0)  # noqa
        btnSizer.Add(self.btnTest, 0, ALL | EXPAND | ALIGN_CENTER_VERTICAL | ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        # self.btnSave = Button(self, ID_ANY, 'Save Connection', DefaultPosition, DefaultSize, 0)  # noqa
        # btnSizer.Add(self.btnSave, 0, ALL | EXPAND | ALIGN_CENTER_VERTICAL | ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        # self.btnCancel = Button(self, ID_ANY, 'Cancel', DefaultPosition, DefaultSize, 0)  # noqa
        # btnSizer.Add(self.btnCancel, 0, ALL | EXPAND | ALIGN_CENTER_VERTICAL | ALIGN_CENTER_HORIZONTAL, 5)  # noqa

        formSizer.Add(btnSizer, 10, EXPAND, 2)

        self.SetSizer(formSizer)
        self.Layout()

        # Connect Events.
        self.btnTest.Bind(EVT_BUTTON, self.OnBtnTest)
        # self.btnSave.Bind(EVT_BUTTON, self.OnBtnSave)
        # self.btnCancel.Bind(EVT_BUTTON, self.OnBtnCancel)
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
