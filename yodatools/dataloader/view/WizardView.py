import wx


class WizardView(wx.Frame):
    def __init__(self, parent):
        super(WizardView, self).__init__(parent)

        panel = wx.Panel(self)

        header_panel = wx.Panel(panel)
        self.body_panel = wx.Panel(panel)
        self.footer_panel = wx.Panel(panel)

        ########################
        # HEADER
        ########################

        # Components
        break_line_header = wx.StaticLine(header_panel)
        self.title_text = wx.StaticText(header_panel, label='Wizard Title')

        # Style components
        title_font = wx.Font(pointSize=18, family=wx.DEFAULT, style=wx.NORMAL, weight=wx.NORMAL)  # noqa
        self.title_text.SetFont(title_font)

        # Sizer
        header_sizer = wx.BoxSizer(wx.VERTICAL)
        header_sizer.Add(self.title_text, 0, wx.EXPAND | wx.ALL, 10)
        header_sizer.Add(break_line_header, 0, wx.EXPAND, 0)

        header_panel.SetSizer(header_sizer)

        ########################
        # BODY
        ########################

        # Components
        self.wizard_pages = []
        self.page_number = 0

        # Sizer
        self.body_sizer = wx.BoxSizer(wx.VERTICAL)

        # Add components to sizer
        # pages are added in the 'add_page' method

        self.body_panel.SetSizer(self.body_sizer)

        ########################
        # FOOTER
        ########################

        # Components
        break_line_footer = wx.StaticLine(self.footer_panel)
        self.next_button = wx.Button(self.footer_panel, label='Next')
        self.back_button = wx.Button(self.footer_panel, label='Back')

        # Sizer
        footer_sizer = wx.BoxSizer(wx.VERTICAL)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Add components to sizer
        button_sizer.Add(self.back_button, 0, wx.EXPAND | wx.ALL, 5)
        button_sizer.Add(self.next_button, 0, wx.EXPAND | wx.ALL, 5)
        footer_sizer.Add(break_line_footer, 0, wx.EXPAND, 0)
        footer_sizer.Add(button_sizer, 0, wx.ALIGN_RIGHT)

        self.footer_panel.SetSizer(footer_sizer)

        self.frame_sizer = wx.BoxSizer(wx.VERTICAL)
        self.frame_sizer.Add(header_panel, 0, wx.EXPAND | wx.ALL, 2)
        self.frame_sizer.Add(self.body_panel, 1, wx.EXPAND | wx.ALL, 2)
        self.frame_sizer.Add(self.footer_panel, 0, wx.EXPAND | wx.ALL, 2)

        panel.SetSizer(self.frame_sizer)

        # Bindings
        self.next_button.Bind(wx.EVT_BUTTON, self.on_next_button)
        self.back_button.Bind(wx.EVT_BUTTON, self.on_back_button)

    def on_next_button(self, event):
        pass

    def on_back_button(self, event):
        pass

    def add_page(self, page):
        self.wizard_pages.append(page)
        self.body_sizer.Add(page, 1, wx.EXPAND | wx.ALL, 0)
