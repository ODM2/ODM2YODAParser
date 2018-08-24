import wx


class WizardExcelPageView(wx.Panel):
    def __init__(self, parent):
        super(WizardExcelPageView, self).__init__(parent)

        # self.SetBackgroundColour(wx.GREEN)
        # self.Hide()

        instructions_text = wx.StaticText(self, label='Choose a location to save Excel export')  # noqa
        self.file_text_ctrl = wx.TextCtrl(self)
        self.browse_button = wx.Button(self, label='Browse')

        # Style components
        self.file_text_ctrl.SetHint('Filepath...')

        # Sizer
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        vertical_sizer = wx.BoxSizer(wx.VERTICAL)
        input_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Add components to vertical_sizer
        input_sizer.Add(self.file_text_ctrl, 1, wx.EXPAND | wx.ALL, 2)
        input_sizer.Add(self.browse_button, 0, wx.EXPAND | wx.ALL, 2)

        vertical_sizer.Add(instructions_text, 0, wx.EXPAND | wx.ALL, 2)
        vertical_sizer.Add(input_sizer, 0, wx.EXPAND | wx.ALIGN_CENTER_HORIZONTAL)  # noqa
        sizer.Add(vertical_sizer, 1, wx.ALIGN_CENTER_VERTICAL | wx.ALL, 16)

        self.SetSizer(sizer)

        self.Hide()
