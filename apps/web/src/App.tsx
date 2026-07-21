import { useEffect, useState, type ReactNode } from "react";
import { Activity, BookOpenCheck, Gauge, LockKeyhole, Settings } from "lucide-react";
import { Cockpit } from "./Cockpit";
import { ReviewPage } from "./ReviewPage";
import { SettingsPage } from "./SettingsPage";

type Route = "cockpit" | "review" | "settings";

function routeFromHash(): Route {
  if (window.location.hash === "#/settings") return "settings";
  if (window.location.hash === "#/review") return "review";
  return "cockpit";
}

export function App() {
  const [route, setRoute] = useState<Route>(routeFromHash);

  useEffect(() => {
    const onHashChange = () => setRoute(routeFromHash());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  return (
    <div className="app-shell">
      <aside className="app-sidebar">
        <a className="brand" href="#/cockpit" aria-label="OptionTrader 驾驶舱首页">
          <span className="brand-mark"><Activity size={19} aria-hidden="true" /></span>
          <span className="brand-copy">
            <strong>OptionTrader</strong>
            <small>波动率交易台</small>
          </span>
        </a>

        <nav className="primary-nav" aria-label="主导航">
          <NavItem href="#/cockpit" active={route === "cockpit"} icon={<Gauge size={18} />}>
            驾驶舱
          </NavItem>
          <NavItem href="#/review" active={route === "review"} icon={<BookOpenCheck size={18} />}>
            每日复盘
          </NavItem>
          <NavItem href="#/settings" active={route === "settings"} icon={<Settings size={18} />}>
            设置
          </NavItem>
        </nav>

        <div className="sidebar-safety">
          <LockKeyhole size={16} aria-hidden="true" />
          <div><strong>故障即闭锁</strong><span>Rust 权威控制</span></div>
        </div>
      </aside>

      <div className="app-workspace">
        {route === "cockpit" ? <Cockpit /> : route === "review" ? <ReviewPage /> : <SettingsPage />}
      </div>

      <nav className="mobile-nav" aria-label="移动端导航">
        <NavItem href="#/cockpit" active={route === "cockpit"} icon={<Gauge size={19} />}>
          驾驶舱
        </NavItem>
        <NavItem href="#/review" active={route === "review"} icon={<BookOpenCheck size={19} />}>
          复盘
        </NavItem>
        <NavItem href="#/settings" active={route === "settings"} icon={<Settings size={19} />}>
          设置
        </NavItem>
      </nav>
    </div>
  );
}

function NavItem({
  href,
  active,
  icon,
  children,
}: {
  href: string;
  active: boolean;
  icon: ReactNode;
  children: ReactNode;
}) {
  return (
    <a className={`nav-item${active ? " active" : ""}`} href={href} aria-current={active ? "page" : undefined}>
      {icon}
      <span>{children}</span>
    </a>
  );
}
