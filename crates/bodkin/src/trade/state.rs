use redb::{Database, DatabaseError, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const POSITIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("positions_v1");
const OPERATIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("operations_v1");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const POSITIONS_KEY: &str = "all";
const SCHEMA_KEY: &str = "schema";
const SCHEMA_VERSION: u64 = 1;

pub struct StateDb {
    database: Database,
    dir: PathBuf,
}

impl StateDb {
    pub fn open(dir: impl AsRef<Path>) -> anyhow::Result<Arc<Self>> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join("bodkin.redb");
        let database = match Database::create(&path) {
            Ok(database) => database,
            Err(DatabaseError::DatabaseAlreadyOpen) => {
                anyhow::bail!(
                    "{} is already owned by another Bodkin engine or live command",
                    path.display()
                )
            }
            Err(error) => return Err(error.into()),
        };
        let mut transaction = database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            transaction.open_table(POSITIONS)?;
            transaction.open_table(OPERATIONS)?;
            let mut meta = transaction.open_table(META)?;
            let schema = meta.get(SCHEMA_KEY)?.map(|version| version.value());
            match schema {
                Some(SCHEMA_VERSION) => {}
                Some(version) => anyhow::bail!(
                    "unsupported state schema {} in {}; expected {}",
                    version,
                    dir.join("bodkin.redb").display(),
                    SCHEMA_VERSION
                ),
                None => {
                    meta.insert(SCHEMA_KEY, SCHEMA_VERSION)?;
                }
            }
        }
        transaction.commit()?;
        Ok(Arc::new(Self {
            database,
            dir: dir.to_path_buf(),
        }))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn load_positions<T: DeserializeOwned>(&self) -> anyhow::Result<Option<T>> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(POSITIONS)?;
        let Some(value) = table.get(POSITIONS_KEY)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(value.value())?))
    }

    pub fn save_positions<T: Serialize>(&self, positions: &T) -> anyhow::Result<()> {
        let encoded = serde_json::to_vec(positions)?;
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut table = transaction.open_table(POSITIONS)?;
            table.insert(POSITIONS_KEY, encoded.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn load_operations<T: DeserializeOwned>(&self) -> anyhow::Result<Vec<T>> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(OPERATIONS)?;
        table
            .iter()?
            .map(|entry| {
                let (_, value) = entry?;
                Ok(serde_json::from_slice(value.value())?)
            })
            .collect()
    }

    pub fn save_operation<T: Serialize>(&self, id: &str, operation: &T) -> anyhow::Result<()> {
        self.save_operations([(id.to_string(), operation)])
    }

    pub fn save_operations<'a, T: Serialize + 'a>(
        &self,
        operations: impl IntoIterator<Item = (String, &'a T)>,
    ) -> anyhow::Result<()> {
        let encoded = operations
            .into_iter()
            .map(|(id, operation)| Ok((id, serde_json::to_vec(operation)?)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        {
            let mut table = transaction.open_table(OPERATIONS)?;
            for (id, value) in &encoded {
                table.insert(id.as_str(), value.as_slice())?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Value {
        id: u64,
    }

    #[test]
    fn transactions_persist_and_the_database_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let state = StateDb::open(dir.path()).unwrap();
        state.save_positions(&vec![Value { id: 1 }]).unwrap();
        state.save_operation("op", &Value { id: 2 }).unwrap();
        assert!(StateDb::open(dir.path()).is_err());
        drop(state);
        let reopened = StateDb::open(dir.path()).unwrap();
        assert_eq!(
            reopened.load_positions::<Vec<Value>>().unwrap().unwrap(),
            vec![Value { id: 1 }]
        );
        assert_eq!(
            reopened.load_operations::<Value>().unwrap(),
            vec![Value { id: 2 }]
        );
        drop(reopened);
    }
}
