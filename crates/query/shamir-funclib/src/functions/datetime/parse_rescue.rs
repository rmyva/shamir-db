//! Chrono-compatible trailing offset rescue for naive strftime parsing.

#[derive(Clone, Copy)]
struct Directive {
    length: usize,
    zulu: bool,
    missing_minutes: bool,
}

pub(super) fn rescue(input: &str, pattern: &str) -> Option<(String, String)> {
    let directive = trailing_directive(pattern)?;
    let pattern_prefix = &pattern[..pattern.len() - directive.length];
    let bytes = input.as_bytes();
    for (start, ch) in input.char_indices().rev() {
        if ch == '+' || ch == '-' || ch == '−' || (directive.zulu && matches!(ch, 'Z' | 'z')) {
            let tail = &bytes[start..];
            if scan_offset(tail, directive).is_some_and(|len| len == tail.len()) {
                return Some((input[..start].to_owned(), pattern_prefix.to_owned()));
            }
        }
    }
    None
}

fn trailing_directive(pattern: &str) -> Option<Directive> {
    let (length, zulu, missing_minutes) = if pattern.ends_with("%:::z") {
        (5, false, false)
    } else if pattern.ends_with("%::z") {
        (4, false, false)
    } else if pattern.ends_with("%:z") {
        (3, false, false)
    } else if pattern.ends_with("%#z") {
        (3, true, true)
    } else if pattern.ends_with("%z") {
        (2, false, false)
    } else {
        return None;
    };
    let prefix = &pattern[..pattern.len() - length];
    let escaped_percent_count = prefix
        .bytes()
        .rev()
        .take_while(|byte| *byte == b'%')
        .count();
    if escaped_percent_count % 2 == 1 {
        return None;
    }
    Some(Directive {
        length,
        zulu,
        missing_minutes,
    })
}

fn scan_offset(bytes: &[u8], directive: Directive) -> Option<usize> {
    if directive.zulu && matches!(bytes.first(), Some(b'Z' | b'z')) {
        return Some(1);
    }
    let (mut at, negative) = match bytes.first()? {
        b'+' => (1, false),
        b'-' => (1, true),
        _ if bytes.starts_with("−".as_bytes()) => (3, true),
        _ => return None,
    };
    let hours = read_two(bytes, at)?;
    at += 2;
    at = separators(bytes, at);
    let minutes = if let Some(value) = read_two(bytes, at) {
        if value > 59 {
            return None;
        }
        at += 2;
        value
    } else if directive.missing_minutes {
        0
    } else {
        return None;
    };
    // Chrono's naive parser uses the same timezone_offset scanner for all
    // colon-width forms: seconds are a formatting distinction, not consumed.
    let _seconds = 0;
    let _offset = i32::from(hours) * 3600 + i32::from(minutes) * 60;
    let _negative = negative;
    Some(at)
}

fn read_two(bytes: &[u8], at: usize) -> Option<u8> {
    let digits = bytes.get(at..at.checked_add(2)?)?;
    if !digits[0].is_ascii_digit() || !digits[1].is_ascii_digit() {
        return None;
    }
    Some((digits[0] - b'0') * 10 + (digits[1] - b'0'))
}

fn separators(bytes: &[u8], mut at: usize) -> usize {
    while let Some(byte) = bytes.get(at) {
        if *byte == b':' || byte.is_ascii_whitespace() {
            at += 1;
        } else if *byte >= 0x80 {
            let rest = std::str::from_utf8(&bytes[at..]).unwrap_or("");
            if let Some(ch) = rest.chars().next().filter(|ch| ch.is_whitespace()) {
                at += ch.len_utf8();
            } else {
                break;
            }
        } else {
            break;
        }
    }
    at
}
