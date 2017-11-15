# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl, StaticLine, Frame, Gauge, \
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, HORIZONTAL,\
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON, GROW, \
    DEFAULT, NORMAL





class WizardSummaryPageView(Panel):
    def __init__(self, parent):
        super(WizardSummaryPageView, self).__init__(parent)

        # Components
        self.gauge = Gauge(self, range=100)


        # Sizers
        sizer = BoxSizer(VERTICAL)

        # Add components to sizer
        sizer.Add(self.gauge, 0, EXPAND | ALL, 5)

        self.SetSizer(sizer)
        self.Hide()
