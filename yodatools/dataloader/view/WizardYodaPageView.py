import wx


class WizardYodaPageView(wx.Panel):
    def __init__(self, parent):
        super(WizardYodaPageView, self).__init__(parent)

        # Components
        instructions_text = wx.StaticText(self, label='Choose a location to save YODA export')  # noqa
        self.file_text_ctrl = wx.TextCtrl(self)
        self.browse_button = wx.Button(self, label='Browse')

        # Style components
        self.file_text_ctrl.SetHint('Choose a directory...')

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

        # Bindings
        self.browse_button.Bind(wx.EVT_BUTTON, self.on_browse_button)

    def on_browse_button(self, event):
        pass
