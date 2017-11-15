# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl,\
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, GROW, \
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON

from yodatools.dataloader.controller.pnlDBConfig import pnlDBConfig


class WizardDatabasePageView(Panel):
    def __init__(self, parent):
        super(WizardDatabasePageView, self).__init__(parent)

        self.panel = pnlDBConfig(self,  is_main=False)
        self.sizer = BoxSizer(VERTICAL)
        self.sizer.Add(self.panel, 1, border=1, flag=EXPAND | GROW | ALL)  # noqa
        self.SetSizer(self.sizer)
        self.sizer.Fit(self.panel)
        # Components
        # instructions_text = StaticText(self, label='Connect to a database')
        # self.database_combo = ComboBox(self, style=CB_READONLY)
        # self.address_text_ctrl = TextCtrl(self)
        # self.username_text_ctrl = TextCtrl(self)
        # self.password_text_ctrl = TextCtrl(self, style=TE_PASSWORD)
        #
        # # Style componets
        # self.address_text_ctrl.SetHint('Address')
        # self.username_text_ctrl.SetHint('Username')
        # self.password_text_ctrl.SetHint('Password')
        #
        # # Sizer
        # sizer = BoxSizer(VERTICAL)
        # horizontal_sizer = BoxSizer(HORIZONTAL)
        #
        # # Add Components to sizer
        # sizer.Add(instructions_text, 0, EXPAND | LEFT | RIGHT, 15)
        # sizer.Add(self.database_combo, 0, EXPAND | ALL ^ BOTTOM ^ TOP, 15)
        # sizer.Add(self.address_text_ctrl, 0, EXPAND | ALL ^ BOTTOM, 15)
        # sizer.Add(self.username_text_ctrl, 0, EXPAND | ALL ^ BOTTOM, 15)
        # sizer.Add(self.password_text_ctrl, 0, EXPAND | ALL ^ BOTTOM, 15)
        #
        # horizontal_sizer.Add(sizer, 1, ALIGN_CENTER_VERTICAL)
        #
        # self.SetSizer(horizontal_sizer)
        # self.Hide()
