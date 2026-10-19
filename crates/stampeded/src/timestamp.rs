//! jiff timestamps across the sqlx boundary.
//!
//! Nullable columns use `#[sqlx(try_from = "NullableTimestamp")]`;
//! the sqlx derive has no `Option` case.

use sqlx::error::BoxDynError;
use sqlx::postgres::{PgTypeInfo, PgValueRef, Postgres};

/// A `timestamptz` column that may be NULL. Decode-only.
#[doc(hidden)]
pub struct NullableTimestamp(Option<jiff_sqlx::Timestamp>);

impl sqlx::Type<Postgres> for NullableTimestamp {
    fn type_info() -> PgTypeInfo {
        <jiff_sqlx::Timestamp as sqlx::Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <jiff_sqlx::Timestamp as sqlx::Type<Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, Postgres> for NullableTimestamp {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        <Option<jiff_sqlx::Timestamp> as sqlx::Decode<'r, Postgres>>::decode(value).map(Self)
    }
}

impl From<NullableTimestamp> for Option<jiff::Timestamp> {
    fn from(value: NullableTimestamp) -> Self {
        value.0.map(jiff_sqlx::Timestamp::to_jiff)
    }
}
