//! Shared query pagination DTOs used by read-side services.

use serde::{Deserialize, Serialize};

use crate::error::IdentityError;

/// Raw page request accepted by query APIs before normalization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PageRequest {
    /// Optional caller-requested page size.
    pub limit: Option<u32>,
    /// Optional opaque cursor used to continue pagination.
    pub cursor: Option<String>,
}

/// Validated page request used by repositories and read services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedPageRequest {
    /// Effective page size after applying defaults and bounds.
    pub limit: u32,
    /// Optional non-blank opaque cursor.
    pub cursor: Option<String>,
}

impl PageRequest {
    /// Normalizes raw page input with the provided defaults and hard maximum.
    pub fn normalize(
        &self,
        default_limit: u32,
        max_limit: u32,
    ) -> Result<NormalizedPageRequest, IdentityError> {
        let limit = match self.limit.unwrap_or(default_limit) {
            0 => {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_INVALID_ARGUMENT",
                    message: "page.limit must be greater than zero".to_string(),
                });
            }
            limit => limit.min(max_limit),
        };
        let cursor = self.cursor.as_ref().map(|value| value.trim().to_string());
        if matches!(cursor.as_deref(), Some("")) {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "page.cursor must not be blank".to_string(),
            });
        }

        Ok(NormalizedPageRequest { limit, cursor })
    }
}

#[cfg(test)]
mod tests {
    use super::PageRequest;
    use crate::error::IdentityError;

    #[test]
    fn normalize_rejects_zero_limit() {
        let error = PageRequest {
            limit: Some(0),
            cursor: None,
        }
        .normalize(20, 100)
        .expect_err("zero limit should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                ..
            }
        ));
    }

    #[test]
    fn normalize_clamps_limit_and_trims_cursor() {
        let page = PageRequest {
            limit: Some(500),
            cursor: Some("  cursor-1  ".to_string()),
        }
        .normalize(20, 100)
        .expect("page request should normalize");

        assert_eq!(page.limit, 100);
        assert_eq!(page.cursor.as_deref(), Some("cursor-1"));
    }
}
