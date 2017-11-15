

# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl,\
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, HORIZONTAL,\
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON, GROW



from yodatools.dataloader.controller.pnlDBConfig import pnlDBConfig


class WizardSQLitePageView(Panel):
    def __init__(self, parent):
        super(WizardSQLitePageView, self).__init__(parent)

        self.panel = pnlDBConfig(self, service_manager=None, is_main=False)
        self.sizer = BoxSizer(VERTICAL)
        self.sizer.Add(self.panel, 1, border=1, flag=EXPAND | GROW | ALL)  # noqa
        self.SetSizer(self.sizer)
        self.sizer.Fit(self.panel)