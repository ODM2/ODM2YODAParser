# import wx
from wx import Panel, Button, StaticBox, ComboBox, StaticText, TextCtrl, StaticLine, Frame, \
    BoxSizer, StaticBoxSizer, FlexGridSizer, \
    DefaultPosition, DefaultSize, Size, EmptyString, Font, \
    ID_ANY, ALL, SIMPLE_BORDER, TAB_TRAVERSAL, VERTICAL, FLEX_GROWMODE_ALL, EXPAND, ALIGN_RIGHT, NORMAL_FONT, HORIZONTAL,\
    CB_READONLY, FULL_REPAINT_ON_RESIZE, ALIGN_CENTER_VERTICAL,  ALIGN_CENTER_HORIZONTAL, TE_PASSWORD, EVT_BUTTON, GROW, \
    DEFAULT, NORMAL




class WizardView(Frame):
    def __init__(self, parent):
        super(WizardView, self).__init__(parent)

        panel = Panel(self)

        header_panel = Panel(panel)
        self.body_panel = Panel(panel)
        self.footer_panel = Panel(panel)

        ########################
        # HEADER
        ########################

        # Components
        break_line_header = StaticLine(header_panel)
        self.title_text = StaticText(header_panel, label='Wizard Title')

        # Style components
        title_font = Font(pointSize=18, family=DEFAULT, style=NORMAL, weight=NORMAL)  # noqa
        self.title_text.SetFont(title_font)

        # Sizer
        header_sizer = BoxSizer(VERTICAL)
        header_sizer.Add(self.title_text, 0, EXPAND | ALL, 10)
        header_sizer.Add(break_line_header, 0, EXPAND, 0)

        header_panel.SetSizer(header_sizer)

        ########################
        # BODY
        ########################

        # Components
        self.wizard_pages = []
        self.page_number = 0

        # Sizer
        self.body_sizer = BoxSizer(VERTICAL)

        # Add components to sizer
        # pages are added in the 'add_page' method

        self.body_panel.SetSizer(self.body_sizer)

        ########################
        # FOOTER
        ########################

        # Components
        break_line_footer = StaticLine(self.footer_panel)
        self.next_button = Button(self.footer_panel, label='Next')
        self.back_button = Button(self.footer_panel, label='Back')

        # Sizer
        footer_sizer = BoxSizer(VERTICAL)
        button_sizer = BoxSizer(HORIZONTAL)

        # Add components to sizer
        button_sizer.Add(self.back_button, 0, EXPAND | ALL, 5)
        button_sizer.Add(self.next_button, 0, EXPAND | ALL, 5)
        footer_sizer.Add(break_line_footer, 0, EXPAND, 0)
        footer_sizer.Add(button_sizer, 0, ALIGN_RIGHT)

        self.footer_panel.SetSizer(footer_sizer)

        self.frame_sizer = BoxSizer(VERTICAL)
        self.frame_sizer.Add(header_panel, 0, EXPAND | ALL, 2)
        self.frame_sizer.Add(self.body_panel, 1, EXPAND | ALL, 2)
        self.frame_sizer.Add(self.footer_panel, 0, EXPAND | ALL, 2)

        panel.SetSizer(self.frame_sizer)

        # Bindings
        self.next_button.Bind(EVT_BUTTON, self.on_next_button)
        self.back_button.Bind(EVT_BUTTON, self.on_back_button)

    def on_next_button(self, event):
        pass

    def on_back_button(self, event):
        pass

    def add_page(self, page):
        self.wizard_pages.append(page)
        self.body_sizer.Add(page, 1, EXPAND | ALL, 0)
