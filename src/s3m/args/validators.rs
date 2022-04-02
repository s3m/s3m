use std::fs;

pub fn is_num(s: &str) -> Result<(), String> {
    if let Err(..) = s.parse::<u64>() {
        return Err(String::from("Not a valid number!"));
    }
    Ok(())
}

pub fn is_file(s: &str) -> Result<(), String> {
    if fs::metadata(&s).map_err(|e| e.to_string())?.is_file() {
        Ok(())
    } else {
        Err(format!(
            "cannot read the file: {}, verify file exist and is not a directory.",
            s
        ))
    }
}

pub fn key_val(s: &str) -> Result<(), String> {
    if s.split(';')
        .map(|s| s.split_once('=').unwrap())
        .map(|(key, val)| (key, val))
        .next()
        .is_none()
    {
        return Err(String::from("metadata format is key1=value1;key2=value2"));
    }
    Ok(())
}
