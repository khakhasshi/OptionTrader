//! Execution-backend configuration and routing policy.

use super::*;

pub(super) fn parse_boolean(
    name: &str,
    value: Option<&str>,
    default: bool,
) -> Result<bool, String> {
    match value {
        Some("true") => Ok(true),
        Some("false") => Ok(false),
        Some(_) => Err(format!("{name} must be exactly true or false")),
        None => Ok(default),
    }
}

pub(super) fn boolean_env(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) => parse_boolean(name, Some(&value), default),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{name} must be valid UTF-8")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BrokerExecutionBackend {
    Disabled,
    SimulatedPaper,
    IbkrPaper,
    LongbridgePaper,
}

impl BrokerExecutionBackend {
    pub(super) fn from_env() -> Result<Self, String> {
        let backend = std::env::var("OPTIONTRADER_BROKER_EXECUTION_BACKEND")
            .unwrap_or_else(|_| "simulated-paper".into());
        let environment = std::env::var("OPTIONTRADER_ENV").unwrap_or_else(|_| "local".into());
        let live_enabled = boolean_env("LIVE_TRADING_ENABLED", false)?;
        let paper_opt_in = boolean_env("OPTIONTRADER_BROKER_PAPER_SUBMISSION_ENABLED", false)?;
        let ibkr_paper = boolean_env("OPTIONTRADER_IBKR_PAPER", true)?;
        let ibkr_submission = boolean_env("OPTIONTRADER_IBKR_SUBMISSION_ENABLED", false)?;
        let longbridge_paper = boolean_env("OPTIONTRADER_LONGBRIDGE_PAPER", false)?;
        let reconciliation_enabled =
            boolean_env("OPTIONTRADER_BROKER_RECONCILIATION_ENABLED", true)?;
        let reconciliation_broker = std::env::var("OPTIONTRADER_BROKER_RECONCILIATION_BROKERS")
            .unwrap_or_else(|_| "ibkr".into());
        Self::from_config(
            &backend,
            &environment,
            live_enabled,
            paper_opt_in,
            ibkr_paper,
            ibkr_submission,
            longbridge_paper,
        )
        .and_then(|backend| {
            backend.require_reconciliation_route(reconciliation_enabled, &reconciliation_broker)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_config(
        backend: &str,
        environment: &str,
        live_enabled: bool,
        paper_opt_in: bool,
        ibkr_paper: bool,
        ibkr_submission: bool,
        longbridge_paper: bool,
    ) -> Result<Self, String> {
        if live_enabled {
            return Err("Phase 3 requires LIVE_TRADING_ENABLED=false".into());
        }
        match backend {
            "disabled" => Ok(Self::Disabled),
            "simulated-paper" => Ok(Self::SimulatedPaper),
            "ibkr-paper"
                if environment == "paper"
                    && paper_opt_in
                    && ibkr_paper
                    && ibkr_submission =>
            {
                Ok(Self::IbkrPaper)
            }
            "longbridge-paper" if environment == "paper" && paper_opt_in && longbridge_paper => {
                Ok(Self::LongbridgePaper)
            }
            "ibkr-paper" | "longbridge-paper" => Err(
                "real paper execution requires paper environment and every broker opt-in".into(),
            ),
            _ => Err(
                "OPTIONTRADER_BROKER_EXECUTION_BACKEND must be disabled, simulated-paper, ibkr-paper, or longbridge-paper"
                    .into(),
            ),
        }
    }

    pub(super) fn allows(self, mode: ProtoMode, broker: ProtoBrokerId) -> bool {
        if matches!(mode, ProtoMode::Replay | ProtoMode::Shadow) {
            return true;
        }
        match self {
            Self::Disabled => false,
            Self::SimulatedPaper => matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm),
            Self::IbkrPaper => {
                broker == ProtoBrokerId::Ibkr
                    && matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm)
            }
            Self::LongbridgePaper => {
                broker == ProtoBrokerId::Longbridge
                    && matches!(mode, ProtoMode::Paper | ProtoMode::ManualConfirm)
            }
        }
    }

    pub(super) fn is_external(self) -> bool {
        matches!(self, Self::IbkrPaper | Self::LongbridgePaper)
    }

    pub(super) fn require_reconciliation_route(
        self,
        reconciliation_enabled: bool,
        reconciliation_broker: &str,
    ) -> Result<Self, String> {
        let expected = match self {
            Self::IbkrPaper => Some("ibkr"),
            Self::LongbridgePaper => Some("longbridge"),
            Self::Disabled | Self::SimulatedPaper => None,
        };
        if expected
            .is_some_and(|broker| !reconciliation_enabled || reconciliation_broker.trim() != broker)
        {
            return Err(
                "real paper execution requires enabled reconciliation for the same broker".into(),
            );
        }
        Ok(self)
    }

    pub(super) fn is_simulated(self) -> bool {
        self == Self::SimulatedPaper
    }

    pub(super) fn broker_route(self) -> Option<ProtoBrokerId> {
        match self {
            Self::IbkrPaper => Some(ProtoBrokerId::Ibkr),
            Self::LongbridgePaper => Some(ProtoBrokerId::Longbridge),
            Self::Disabled | Self::SimulatedPaper => None,
        }
    }
}
