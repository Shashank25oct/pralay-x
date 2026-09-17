import type { ReactNode } from 'react';
export default function Badge({children,type='info'}:{children:ReactNode,type?:string}){return <span className={`badge ${type.toLowerCase()}`}>{children}</span>}
