use crate::style::{muted, neon, on_neon};

pub struct Links {
    pub axiom_handle: String,
    pub fomo_handle: String,
}

impl Links {
    pub fn from_env() -> Self {
        Self {
            axiom_handle: crate::config::env_str("REF_AXIOM").unwrap_or_else(|| "phosphen".into()),
            fomo_handle: crate::config::env_str("REF_FOMO").unwrap_or_else(|| "phosphenq".into()),
        }
    }

    pub fn pons(token: &str) -> String {
        format!("https://www.ponsfamily.com/token/{token}")
    }
    pub fn explorer(token: &str) -> String {
        format!("https://robinhoodchain.blockscout.com/token/{token}")
    }
    pub fn axiom(curve: &str) -> String {
        format!(
            "https://axiom.trade/meme/{}?chain=robinhood",
            curve.to_ascii_lowercase()
        )
    }
    pub fn fomo(token: &str) -> String {
        format!(
            "https://fomo.family/tokens/robinhood/{}",
            token.to_ascii_lowercase()
        )
    }
    pub fn axiom_ref(&self) -> String {
        if self.axiom_handle.is_empty() {
            String::new()
        } else {
            format!("https://axiom.trade/@{}", self.axiom_handle)
        }
    }
    pub fn fomo_ref(&self) -> String {
        if self.fomo_handle.is_empty() {
            String::new()
        } else {
            format!("https://fomo.family/r/{}", self.fomo_handle)
        }
    }
}

pub fn osc(text: &str, url: &str) -> String {
    let tty = std::io::IsTerminal::is_terminal(&std::io::stdout())
        || std::env::var_os("FORCE_COLOR").is_some();
    if url.is_empty() || std::env::var_os("NO_COLOR").is_some() || !tty {
        return text.to_string();
    }
    format!("\x1b]8;;{url}\x1b\\{text}\x1b]8;;\x1b\\")
}

pub fn ref_line(links: &Links) -> String {
    let mut parts = Vec::new();
    if !links.axiom_ref().is_empty() {
        parts.push(neon(osc("axiom", &links.axiom_ref())));
    }
    if !links.fomo_ref().is_empty() {
        parts.push(neon(osc("fomo", &links.fomo_ref())));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{} {}", on_neon(" sign up "), parts.join(&muted(" · ")))
    }
}
