use std::io::IsTerminal;

fn color_on() -> bool {
    std::env::var_os("NO_COLOR").is_none()
        && (std::io::stdout().is_terminal() || std::env::var_os("FORCE_COLOR").is_some())
}

fn wrap(open: &str, s: impl std::fmt::Display) -> String {
    if color_on() {
        format!("{open}{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

pub fn neon(s: impl std::fmt::Display) -> String {
    wrap("\x1b[38;2;204;255;0m", s)
}
pub fn on_neon(s: impl std::fmt::Display) -> String {
    wrap("\x1b[48;2;204;255;0m\x1b[38;2;17;14;8m", s)
}
pub fn white(s: impl std::fmt::Display) -> String {
    wrap("\x1b[38;2;255;255;255m", s)
}
pub fn muted(s: impl std::fmt::Display) -> String {
    wrap("\x1b[38;2;150;150;150m", s)
}
pub fn loss(s: impl std::fmt::Display) -> String {
    wrap("\x1b[38;2;255;107;92m", s)
}

pub fn info(msg: impl std::fmt::Display) {
    println!("{msg}");
}
pub fn warn(msg: impl std::fmt::Display) {
    eprintln!("{} {msg}", loss("!"));
}
pub fn error(msg: impl std::fmt::Display) {
    eprintln!("{} {msg}", loss("✗"));
}
pub fn hr(width: usize) -> String {
    muted("─".repeat(width))
}

const LINES: [&str; 6] = [
    "██████╗  ██████╗ ██████╗ ██╗  ██╗██╗███╗   ██╗",
    "██╔══██╗██╔═══██╗██╔══██╗██║ ██╔╝██║████╗  ██║",
    "██████╔╝██║   ██║██║  ██║█████╔╝ ██║██╔██╗ ██║",
    "██╔══██╗██║   ██║██║  ██║██╔═██╗ ██║██║╚██╗██║",
    "██████╔╝╚██████╔╝██████╔╝██║  ██╗██║██║ ╚████║",
    "╚═════╝  ╚═════╝ ╚═════╝ ╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝",
];
const GRADIENT: [[u8; 3]; 6] = [
    [204, 255, 0],
    [214, 250, 0],
    [224, 246, 0],
    [235, 241, 0],
    [245, 236, 0],
    [255, 231, 0],
];

pub fn banner(tagline: &str) {
    if !color_on() {
        println!("\n{}\n  {tagline}\n", LINES.join("\n"));
        return;
    }
    println!();
    for (i, l) in LINES.iter().enumerate() {
        let [r, g, b] = GRADIENT[i];
        println!("\x1b[38;2;{r};{g};{b}m{l}\x1b[0m");
    }
    println!("\x1b[38;2;150;150;150m  {tagline}\x1b[0m\n");
}
