use graphannis::update::{GraphUpdate,GraphUpdateIterator,UpdateEvent};
use crate::error::{Result,AnnattoError};
use serde::{Serialize,Serializer};
use std::{fs::File, io::Write};


fn event_to_string(update_event: &UpdateEvent) -> Result<String> {
    Ok(format!("{:?}",update_event))
}


pub fn write_to_file(updates: &GraphUpdate, path: &std::path::Path) -> Result<()> {
    let mut file = File::create(path)?;    
    let it = updates.iter()?;
    for update_event in it {
        let event_tuple = update_event?;        
        let event_string = event_to_string(&event_tuple.1)?;
        file.write(event_string.as_bytes())?;
        file.write(b"\n")?;        
    };
    Ok(())
}