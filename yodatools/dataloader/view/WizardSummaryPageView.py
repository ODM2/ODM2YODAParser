import wx


class WizardSummaryPageView(wx.Panel):
    def __init__(self, parent):
        super(WizardSummaryPageView, self).__init__(parent)

        # Components
        self.gauge = wx.Gauge(self, range=100)
        self.gauge_label = wx.StaticText(self, wx.ID_ANY, '')
        self.gauge2 = wx.Gauge(self, range=100)
        self.gauge2_label = wx.StaticText(self, wx.ID_ANY, '')

        self.output = wx.TextCtrl(self, wx.ID_ANY, '', style=wx.TE_MULTILINE | wx.TE_READONLY)
        self.output.SetCursor(wx.Cursor(wx.CURSOR_ARROW))

        # Sizers
        vbox = wx.BoxSizer(wx.VERTICAL)
        output_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Add components to sizer
        vbox.Add(self.gauge, flag=wx.EXPAND | wx.ALL, border=5)
        vbox.Add(self.gauge_label, flag=wx.EXPAND | wx.ALL, border=5)
        vbox.Add(self.gauge2, flag=wx.EXPAND | wx.ALL, border=5)
        vbox.Add(self.gauge2_label, flag=wx.EXPAND | wx.ALL, border=5)

        output_sizer.Add(self.output, proportion=1, flag=wx.EXPAND, border=5)

        vbox.Add(output_sizer, proportion=1, flag=wx.EXPAND | wx.LEFT | wx.RIGHT, border=5)

        self.SetSizer(vbox)
        self.Hide()
