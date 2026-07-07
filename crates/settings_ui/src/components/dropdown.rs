use std::rc::Rc;

use gpui::{App, ElementId, IntoElement, RenderOnce, SharedString};
use heck::ToTitleCase as _;
use ui::{
    ButtonSize, ContextMenu, Disableable as _, DropdownMenu, DropdownStyle, FluentBuilder as _,
    IconPosition, px,
};

fn gearbox_dropdown_label(label: &str, should_do_title_case: bool) -> String {
    let label = if should_do_title_case {
        label.to_title_case()
    } else {
        label.to_string()
    };

    if std::env::var("GEARBOX_GUI").as_deref() != Ok("1") {
        return label;
    }

    match label.as_str() {
        "Empty Tab" => "空白标签页".to_string(),
        "Last Workspace" => "上次工作区".to_string(),
        "Last Session" => "上次会话".to_string(),
        "Launchpad" => "启动页".to_string(),
        "Always" => "始终".to_string(),
        "Never" => "从不".to_string(),
        "On" => "开启".to_string(),
        "Off" => "关闭".to_string(),
        "System" => "跟随系统".to_string(),
        "Light" => "浅色".to_string(),
        "Dark" => "深色".to_string(),
        _ => label.replace("Zed", "Gearbox"),
    }
}

#[derive(IntoElement)]
pub struct EnumVariantDropdown<T>
where
    T: strum::VariantArray + strum::VariantNames + Copy + PartialEq + Send + Sync + 'static,
{
    id: ElementId,
    current_value: T,
    variants: &'static [T],
    labels: &'static [&'static str],
    should_do_title_case: bool,
    tab_index: Option<isize>,
    disabled: bool,
    aria_label: Option<SharedString>,
    on_change: Rc<dyn Fn(T, &mut ui::Window, &mut App) + 'static>,
}

impl<T> EnumVariantDropdown<T>
where
    T: strum::VariantArray + strum::VariantNames + Copy + PartialEq + Send + Sync + 'static,
{
    pub fn new(
        id: impl Into<ElementId>,
        current_value: T,
        variants: &'static [T],
        labels: &'static [&'static str],
        on_change: impl Fn(T, &mut ui::Window, &mut App) + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            current_value,
            variants,
            labels,
            should_do_title_case: true,
            tab_index: None,
            disabled: false,
            aria_label: None,
            on_change: Rc::new(on_change),
        }
    }

    pub fn title_case(mut self, title_case: bool) -> Self {
        self.should_do_title_case = title_case;
        self
    }

    pub fn tab_index(mut self, tab_index: isize) -> Self {
        self.tab_index = Some(tab_index);
        self
    }

    pub fn disabled(mut self, disabled: bool) -> Self {
        self.disabled = disabled;
        self
    }

    /// Sets the label announced by assistive technology.
    /// Defaults to the currently selected value's label.
    pub fn aria_label(mut self, label: impl Into<SharedString>) -> Self {
        self.aria_label = Some(label.into());
        self
    }
}

impl<T> RenderOnce for EnumVariantDropdown<T>
where
    T: strum::VariantArray + strum::VariantNames + Copy + PartialEq + Send + Sync + 'static,
{
    fn render(self, window: &mut ui::Window, cx: &mut ui::App) -> impl gpui::IntoElement {
        let current_value_label = self.labels[self
            .variants
            .iter()
            .position(|v| *v == self.current_value)
            .unwrap()];

        let context_menu = window.use_keyed_state(current_value_label, cx, |window, cx| {
            ContextMenu::new(window, cx, move |mut menu, _, _| {
                for (&value, &label) in std::iter::zip(self.variants, self.labels) {
                    let on_change = self.on_change.clone();
                    let current_value = self.current_value;
                    menu = menu.toggleable_entry(
                        gearbox_dropdown_label(label, self.should_do_title_case),
                        value == current_value,
                        IconPosition::End,
                        None,
                        move |window, cx| {
                            on_change(value, window, cx);
                        },
                    );
                }
                menu
            })
        });

        DropdownMenu::new(
            self.id,
            gearbox_dropdown_label(current_value_label, self.should_do_title_case),
            context_menu,
        )
        .when_some(self.aria_label, |this, label| this.aria_label(label))
        .disabled(self.disabled)
        .when_some(self.tab_index, |elem, tab_index| elem.tab_index(tab_index))
        .trigger_size(ButtonSize::Medium)
        .style(DropdownStyle::Outlined)
        .offset(gpui::Point {
            x: px(0.0),
            y: px(2.0),
        })
        .into_any_element()
    }
}
