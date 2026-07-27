diff --git a/src/redis.rs b/src/redis.rs
index 59b1815..daf1eb9 100644
--- a/src/redis.rs
+++ b/src/redis.rs
@@ -1,7 +1,6 @@
 use std::{
     collections::{HashMap, VecDeque},
     fmt::Display,
-    str::FromStr,
     sync::{Arc, Condvar},
     time::Instant,
 };
@@ -33,21 +32,6 @@ impl StreamId {
     pub const ZERO: Self = Self { ms: 0, sq: 0 };
 }
 
-impl FromStr for StreamId {
-    type Err = RedisError;
-    fn from_str(s: &str) -> Result<Self, Self::Err> {
-        let (ms, sq) = s.split_once('-').ok_or(RedisError::StreamIdParseError)?;
-        Ok(StreamId {
-            ms: ms
-                .parse::<u64>()
-                .map_err(|_| RedisError::StreamIdParseError)?,
-            sq: sq
-                .parse::<u64>()
-                .map_err(|_| RedisError::StreamIdParseError)?,
-        })
-    }
-}
-
 #[derive(Clone)]
 pub struct StreamEntry {
     id: StreamId,
@@ -97,6 +81,29 @@ impl Stream {
         }
         Ok(())
     }
+
+    fn parse_stream_id(&self, s: &str) -> Result<StreamId, RedisError> {
+        let (ms, sq) = s.split_once('-').ok_or(RedisError::StreamIdParseError)?;
+        let ms = ms
+            .parse::<u64>()
+            .map_err(|_| RedisError::StreamIdParseError)?;
+        let sq = if sq != "*" {
+            sq.parse::<u64>()
+                .map_err(|_| RedisError::StreamIdParseError)?
+        } else {
+            match self.entries.last() {
+                Some(last) => last.id.sq + 1,
+                None => {
+                    if ms == 0 {
+                        1
+                    } else {
+                        0
+                    }
+                }
+            }
+        };
+        Ok(StreamId { ms, sq })
+    }
 }
 
 #[derive(Clone)]
@@ -256,25 +263,25 @@ impl Redis {
         }
     }
     pub fn x_add(&mut self, key: &str, id: &str, args: &[String]) -> Result<String, RedisError> {
-        let stream_id = StreamId::from_str(id)?;
+        let stream = {
+            loop {
+                match self.get_mut_value(key) {
+                    Some(Value::Stream(s)) => break s,
+                    Some(_) => return Err(RedisError::WrongType),
+                    None => {
+                        let stream = Stream::new();
+                        self.store.insert(key.to_string(), Value::Stream(stream));
+                    }
+                }
+            }
+        };
+        let stream_id = stream.parse_stream_id(id)?;
         let mut entry = StreamEntry::new(stream_id);
         for chunk in args.chunks_exact(2) {
             let (k, v) = (&chunk[0], &chunk[1]);
             entry.add(k.clone(), v.clone());
         }
-        match self.get_mut_value(key) {
-            Some(Value::Stream(s)) => {
-                s.validate(entry.id)?;
-                s.add(entry)
-            }
-            Some(_) => return Err(RedisError::WrongType),
-            None => {
-                let mut stream = Stream::new();
-                stream.validate(entry.id)?;
-                stream.add(entry);
-                self.store.insert(key.to_string(), Value::Stream(stream));
-            }
-        }
+        stream.add(entry);
         Ok(id.to_string())
     }
 }
